"""从 HTML 中提取 a[href] 链接。"""
from __future__ import annotations
from html.parser import HTMLParser


class _LinkParser(HTMLParser):

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != 'a':
            return
        for name, value in attrs:
            if name.lower() == 'href' and value and value.strip():
                self.hrefs.append(value.strip())


def extract_links(body: bytes) -> list[str]:
    parser = _LinkParser()
    try:
        parser.feed(body.decode('utf-8', 'replace'))
        parser.close()
    except ValueError:
        pass  # 畸形 HTML 不应拖垮调度
    return parser.hrefs
