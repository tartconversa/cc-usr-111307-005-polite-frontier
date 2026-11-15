import unittest

from src.robots import RobotsRules, ua_token_of

FIXTURE = ('User-agent: DirectoryBot\n'
           'Disallow: /private\n'
           'Allow: /private/open$\n'
           'Disallow: /*?secret=*\n')


class RobotsTests(unittest.TestCase):

    def check(self, text, ua, path_query):
        return RobotsRules.parse(text).check(ua, path_query)

    def test_fixture_rules(self):
        allowed, rule = self.check(FIXTURE, 'directorybot', '/private')
        self.assertFalse(allowed)
        self.assertEqual(rule.pattern, '/private')
        self.assertTrue(self.check(FIXTURE, 'directorybot', '/private/open')[0])
        # $ 锚定结尾：/private/open/2 不适用 Allow，落回 Disallow: /private
        self.assertFalse(self.check(FIXTURE, 'directorybot', '/private/open/2')[0])

    def test_wildcard_pattern(self):
        allowed, rule = self.check(FIXTURE, 'directorybot', '/detail/1?secret=1')
        self.assertFalse(allowed)
        self.assertEqual(rule.pattern, '/*?secret=*')
        self.assertTrue(self.check(FIXTURE, 'directorybot', '/detail/1?utm=1')[0])

    def test_user_agent_group_selection(self):
        # 其他 UA 不匹配 DirectoryBot 组，也没有 * 组：全部允许
        self.assertTrue(self.check(FIXTURE, 'otherbot', '/private')[0])

    def test_star_group_when_no_exact_match(self):
        text = 'User-agent: *\nDisallow: /x\n\nUser-agent: DirectoryBot\nDisallow: /y\n'
        self.assertFalse(self.check(text, 'otherbot', '/x')[0])
        self.assertTrue(self.check(text, 'otherbot', '/y')[0])
        self.assertTrue(self.check(text, 'directorybot', '/x')[0])
        self.assertFalse(self.check(text, 'directorybot', '/y')[0])

    def test_tie_allow_wins(self):
        text = 'User-agent: *\nDisallow: /a\nAllow: /a\n'
        self.assertTrue(self.check(text, 'anybot', '/a')[0])

    def test_longest_match_wins(self):
        text = 'User-agent: *\nDisallow: /a\nAllow: /a/b\n'
        self.assertTrue(self.check(text, 'anybot', '/a/b')[0])
        self.assertFalse(self.check(text, 'anybot', '/a/c')[0])

    def test_wildcard_suffix_and_empty_disallow(self):
        text = 'User-agent: *\nDisallow: /tmp*\nDisallow:\n'
        self.assertFalse(self.check(text, 'anybot', '/tmpfoo')[0])
        self.assertTrue(self.check(text, 'anybot', '/')[0])

    def test_ua_token(self):
        self.assertEqual(ua_token_of('DirectoryBot/1.0 (+info)'), 'directorybot')


if __name__ == '__main__':
    unittest.main()
