from collections import defaultdict
from html.parser import HTMLParser
from urllib.request import (urlopen, Request, ProxyHandler, install_opener,
                            build_opener)
import dateutil
import gzip
import io
import textwrap
import urllib

from tanker import connect, View, yaml_load, create_tables

# Add proxy support
proxy = ProxyHandler({
    'http': 'http://proxy.eib.electrabel.be:8080',
    'https': 'http://proxy.eib.electrabel.be:8080',
})
install_opener(build_opener(proxy))

schema = '''
- table: feed
  columns:
    link: varchar
    title: varchar
    description: varchar
  key:
    - link
- table: feed_item
  columns:
    link: varchar
    pubdate: timestamp
    title: varchar
    description: varchar
    text: varchar
    extra: jsonb
  key:
    - link
'''

cfg = {
    # 'db_uri': 'postgresql:///test',
    'db_uri': 'sqlite:///test.db',
    'schema': yaml_load(schema),
}


def get(url):
    ua = 'curl 7.16.1 (i386-portbld-freebsd6.2) libcurl/7.16.1 OpenSSL/0.9.7m zlib/1.2.3'
    # url = 'https://medium.com/@iantien/top-takeaways-from-andy-grove-s-high-output-management-2e0ecfb1ea63'
    resp = urlopen(Request(url, headers={
        'User-Agent': ua,
        'Accept-encoding': 'gzip',
    }))

    if resp.headers.get('Content-Encoding') == 'gzip':
        buf = io.BytesIO(resp.read())
        f = gzip.GzipFile(fileobj=buf)
        content = f.read()
    else:
        content = resp.read()
    return content.decode('utf-8'), resp

class Element:
    '''
    Utility class for TextParser
    '''
    def __init__(self, name, attrs):
        self.name = name.lower()
        self.attrs = dict(attrs)
        self.content = ''

    def __repr__(self):
        return '<%s: %s (%s)>' % (self.name, self.content, self.attrs.items())


class TextParser(HTMLParser):
    '''
    Extract meaningful text from a webpage by identifying elements
    path with the longest average content.
    '''

    def __init__(self):
        self.rows = []
        self.stack = []
        self.skip = set(['script', 'noscript', 'svg', 'img', 'g', 'input',
                         'form', 'html', 'body', 'path', 'style'])
        super().__init__()

    def record(self):
        el = self.stack[-1]
        key = tuple(i.name for i in self.stack)
        self.rows.append((key, el.content))

    def handle_starttag(self, tag, attrs):
        self.stack.append(Element(tag, attrs))

    def handle_endtag(self, tag):
        leaf = self.stack and self.stack[-1]
        while self.stack:
            self.record()
            self.stack.pop()
            if tag == leaf.name:
                break

    def handle_data(self, content):
        content = content.strip()
        if not content:
            return
        if not self.stack:
            return
        leaf = self.stack[-1]
        if leaf.name in self.skip:
            return
        leaf.content += content

    def topN(self, n=3):
        scores = defaultdict(list)
        for k, content in self.rows:
            scores[k].append(len(content))
        board = [(sum(s)/len(s), k) for k, s in scores.items()]
        keep = set(k for s, k in  sorted(board)[-n:])
        for k, content in self.rows:
            if k in keep:
                yield content

    @classmethod
    def get_text(cls, link):
        try:
            content, resp = get(link)
        except (urllib.error.HTTPError, urllib.error.URLError):
            return None
        content_type = resp.headers.get('Content-Type')
        if not content_type.startswith('text/html'):
            return None

        tp = TextParser()
        tp.feed(content)
        return '\n'.join(textwrap.fill(l) for l in tp.topN(3))


class RSSParser(HTMLParser):
    '''
    Parse RSS files
    '''
    # XXX we may need to use xml.sax to be able to extract cdata

    def __init__(self):
        self.stack = []
        self.channel_info = {}
        self.items = []
        self.item_info = {}
        super().__init__()

    def inspect(self):
        prefix = tuple(i.name for i in self.stack[:-1])
        leaf = self.stack[-1]

        if prefix == ('rss', 'channel'):
            if leaf.name == 'item':
                self.items.append(self.item_info)
                self.item_info['feed_link'] = self.channel_info['link']
                self.item_info = {}
            else:
                self.channel_info[leaf.name] = leaf.content
        elif prefix == ('rss', 'channel', 'item'):
            if leaf.name in ('title', 'link', 'pubdate', 'description'):
                if leaf.name == 'pubdate':
                    leaf.content = dateutil.parser.parse(leaf.content)
                elif leaf.name == 'link':
                    link = leaf.content
                    print('\n ----\n')
                    print(link)
                    text = TextParser.get_text(link)
                    print(text and text[:100])
                    self.item_info['text'] = text
                self.item_info[leaf.name] = leaf.content
            else:
                extra = self.item_info.setdefault('extra', {})
                extra[leaf.name] = leaf.content

    def handle_starttag(self, tag, attrs):
        self.stack.append(Element(tag, attrs))

    def handle_endtag(self, tag):
        self.inspect()
        self.stack.pop()

    def handle_data(self, content):
        content = content.strip()
        if not content:
            return
        if not self.stack:
            return
        leaf = self.stack[-1]
        leaf.content += content

if __name__ == '__main__':
    # url = 'http://firstround.com/review/hypergrowth-and-the-law-of-startup-physics'
    # content, resp = get(url)
    # import pdb;pdb.set_trace()
    # print(content)

    parser = RSSParser()
    content, resp = get('https://news.ycombinator.com/rss')
    parser.feed(content)
    with connect(cfg):
        create_tables()
        View('feed_item', {
            'title': 'title',
            'link': 'link',
            'pubdate': 'pubdate',
            'description': 'description',
            'text': 'text',
            'extra': 'extra',
            # TODO add feed.link
        }).write(parser.items)

    # url = 'https://medium.com/@iantien/top-takeaways-from-andy-grove-s-high-output-management-2e0ecfb1ea63'
    # parser = TextParser()
    # parser.feed(source)
    # import pdb;pdb.set_trace()
    # print('done')
