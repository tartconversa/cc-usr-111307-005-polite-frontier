"""HTTP 抓取：不跟随重定向，3xx 作为结果交回调度器逐跳重新准入。"""
from __future__ import annotations
import http.client
import urllib.error
import urllib.request
from dataclasses import dataclass, field


@dataclass
class FetchResult:
    """一次 HTTP 请求的结果；status 为 None 表示网络层失败。"""

    status: int | None
    headers: dict = field(default_factory=dict)
    body: bytes = b''
    error: str | None = None

    def __post_init__(self):
        self.headers = {str(k).lower(): v for k, v in self.headers.items()}

    def header(self, name: str):
        return self.headers.get(name.lower())


class _NoRedirect(urllib.request.HTTPRedirectHandler):

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # 不跟随：3xx 以 HTTPError 形式返回给调用方


class HttpFetcher:
    """真实抓取器：每个请求带配置的 User-Agent，不自动跟随重定向。"""

    def __init__(self, user_agent: str, timeout: float = 10.0, max_body: int = 1 << 20):
        self.user_agent = user_agent
        self.timeout = timeout
        self.max_body = max_body
        self.opener = urllib.request.build_opener(_NoRedirect)

    def fetch(self, url: str) -> FetchResult:
        request = urllib.request.Request(url, headers={'User-Agent': self.user_agent})
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                return FetchResult(response.getcode(), dict(response.headers.items()),
                                   response.read(self.max_body))
        except urllib.error.HTTPError as exc:  # 含 3xx（不跟随）与 4xx/5xx
            try:
                body = exc.read(self.max_body)
            except Exception:
                body = b''
            return FetchResult(exc.code, dict(exc.headers.items()) if exc.headers else {}, body)
        except (urllib.error.URLError, OSError, ValueError, http.client.HTTPException) as exc:
            return FetchResult(None, {}, error='%s: %s' % (type(exc).__name__, exc))
