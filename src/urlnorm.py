"""URL 解析、相对链接解析与规范化。

规范化结果只用于判重：去片段、解析点段、主机小写、统一默认端口、
删除配置列出的跟踪参数。发往站点的请求始终使用解析后的原始 URL，
不重排查询参数、不改写参数值，签名 URL 因此保持有效。
"""
from __future__ import annotations
from urllib.parse import unquote_plus, urljoin, urlsplit, urlunsplit

DEFAULT_PORTS = {'http': 80, 'https': 443}


class UrlError(ValueError):
    """URL 无法解析或不在支持范围内。"""


def _split(url: str):
    try:
        parts = urlsplit(url)
        parts.port  # 触发端口合法性校验
    except ValueError as exc:
        raise UrlError(str(exc)) from exc
    return parts


def resolve_url(base: str, href: str) -> str:
    """把 href 解析为绝对请求 URL：去掉片段，其余字节保持原样。"""
    parts = _split(urljoin(base, href))
    if parts.scheme.lower() not in DEFAULT_PORTS:
        raise UrlError('不支持的协议: %s' % (parts.scheme or '(空)'))
    if not parts.hostname:
        raise UrlError('缺少主机名')
    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ''))


def normalize_url(url: str, tracking_params=frozenset()) -> str:
    """生成判重键：协议与主机小写、去默认端口、解析点段、去片段、删跟踪参数。"""
    parts = _split(url)
    scheme = parts.scheme.lower()
    host = (parts.hostname or '').lower()
    if not host:
        raise UrlError('缺少主机名')
    port = parts.port
    netloc = host if port is None or port == DEFAULT_PORTS.get(scheme) else '%s:%d' % (host, port)
    path = _remove_dot_segments(parts.path) or '/'
    query = _filter_query(parts.query, tracking_params)
    return urlunsplit((scheme, netloc, path, query, ''))


def origin_of(url: str) -> str:
    """URL 所属 origin：协议 + 小写主机 + 非默认端口。"""
    parts = _split(url)
    scheme = parts.scheme.lower()
    host = (parts.hostname or '').lower()
    if not host:
        raise UrlError('缺少主机名')
    port = parts.port
    if port is None or port == DEFAULT_PORTS.get(scheme):
        return '%s://%s' % (scheme, host)
    return '%s://%s:%d' % (scheme, host, port)


def _filter_query(query: str, tracking_params) -> str:
    """仅删除配置列出的跟踪参数；其余段按原顺序、原字节保留（含重复参数）。"""
    if not query or not tracking_params:
        return query
    kept = []
    for segment in query.split('&'):
        key = segment.split('=', 1)[0]
        if unquote_plus(key) in tracking_params:
            continue
        kept.append(segment)
    return '&'.join(kept)


def _remove_dot_segments(path: str) -> str:
    """RFC 3986 5.2.4 点段消除。"""
    if not path:
        return path
    out = []
    rest = path
    while rest:
        if rest.startswith('../'):
            rest = rest[3:]
        elif rest.startswith('./'):
            rest = rest[2:]
        elif rest.startswith('/./'):
            rest = rest[2:]
        elif rest == '/.':
            rest = '/'
        elif rest.startswith('/../'):
            rest = rest[3:]
            if out:
                out.pop()
        elif rest == '/..':
            rest = '/'
            if out:
                out.pop()
        elif rest in ('.', '..'):
            rest = ''
        else:
            end = rest.find('/', 1) if rest.startswith('/') else rest.find('/')
            if end == -1:
                out.append(rest)
                rest = ''
            else:
                out.append(rest[:end])
                rest = rest[end:]
    return ''.join(out)
