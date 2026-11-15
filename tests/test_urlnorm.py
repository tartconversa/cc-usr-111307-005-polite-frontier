import unittest

from src.urlnorm import UrlError, normalize_url, origin_of, resolve_url


class NormalizeTests(unittest.TestCase):

    def test_fragment_case_and_default_port(self):
        self.assertEqual(normalize_url('HTTP://Example.COM:80/A#frag'),
                         'http://example.com/A')
        self.assertEqual(normalize_url('https://Example.com:443/x'), 'https://example.com/x')

    def test_non_default_port_kept(self):
        self.assertEqual(normalize_url('http://h:8080/x'), 'http://h:8080/x')

    def test_dot_segments_resolved(self):
        self.assertEqual(normalize_url('http://h/a/b/../c/./d'), 'http://h/a/c/d')
        self.assertEqual(normalize_url('http://h/a/../../b'), 'http://h/b')

    def test_tracking_removed_rest_order_kept(self):
        key = normalize_url('http://h/p?utm_source=x&b=2&b=1&sig=q', {'utm_source'})
        self.assertEqual(key, 'http://h/p?b=2&b=1&sig=q')

    def test_signed_url_not_rewritten(self):
        # 不重排、不改写：签名参数的字节与顺序原样保留
        url = 'http://h/s?z=1&a=2&sig=%2Fabc&z=3'
        self.assertEqual(normalize_url(url, {'utm'}), url)

    def test_blank_and_duplicate_params_kept(self):
        self.assertEqual(normalize_url('http://h/p?a&a=&b=1', {'utm'}),
                         'http://h/p?a&a=&b=1')

    def test_request_url_keeps_original_query(self):
        # 请求 URL 只去片段，跟踪参数仍发给站点
        self.assertEqual(resolve_url('http://h/', '/p?utm=1&b=2&b=1#f'),
                         'http://h/p?utm=1&b=2&b=1')

    def test_resolve_relative(self):
        self.assertEqual(resolve_url('http://h/dir/page', '../img?x=1#f'),
                         'http://h/img?x=1')
        self.assertEqual(resolve_url('http://h/a/', '//cdnh/x'), 'http://cdnh/x')

    def test_origin(self):
        self.assertEqual(origin_of('http://h:80/x'), 'http://h')
        self.assertEqual(origin_of('http://h:8080/x'), 'http://h:8080')
        self.assertEqual(origin_of('HTTPS://H/x'), 'https://h')

    def test_invalid(self):
        for bad in ('javascript:alert(1)', 'mailto:a@b', 'http://h:bad/x', 'ftp://h/x'):
            with self.assertRaises(UrlError, msg=bad):
                resolve_url('http://h/', bad)


if __name__ == '__main__':
    unittest.main()
