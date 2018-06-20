from collections import defaultdict
from html.parser import HTMLParser
from urllib.request import (urlopen, Request, ProxyHandler, install_opener,
                            build_opener)
import dateutil
import os
import gzip
import io
import logging
import textwrap
import urllib

from tanker import connect, View, yaml_load, create_tables

fmt = '%(levelname)s:%(asctime).19s: %(message)s'
logging.basicConfig(format=fmt)
logger = logging.getLogger('recess')
logger.setLevel('WARN')


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

    def handle_starttag(self, tag, attrs):
        el = Element(tag, attrs)
        self.stack.append(el)

    def handle_endtag(self, tag):
        leaf = self.stack and self.stack[-1]
        while self.stack:
            self.stack.pop()
            if tag == leaf.name:
                break

    def handle_data(self, content):
        content = content.strip()
        if not content:
            return
        if not self.stack:
            return
        key = tuple(i.name for i in self.stack)
        if key[-1] in self.skip:
            return
        self.rows.append((key, content))

    def topN(self, n=3):
        scores = defaultdict(list)
        for k, content in self.rows:
            scores[k].append(len(content))
        board = [(sum(s)/len(s), k) for k, s in scores.items()]
        keep = set(k for s, k in  sorted(board)[-n:])
        match = lambda n: any(x[:len(n)] == n for x in keep)
        for k, content in self.rows:
            if match(k):
                yield content

    @classmethod
    def get_text(cls, link):
        try:
            content, resp = get(link)
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            logger.info('Unable to load %s' % link)
            return None
        content_type = resp.headers.get('Content-Type')
        if not content_type.startswith('text/html'):
            logger.info('Unable to parse %s' % link)
            return None

        tp = TextParser()
        tp.feed(content)
        return '\n'.join(textwrap.fill(l) for l in tp.topN())


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


def auto_proxy():
    # Add proxy support
    handlers = {}
    for variable in ('http_proxy', 'https_proxy'):
        value = os.environ.get('http_proxy')
        if not value:
            continue
        handlers[variable] = value
    if not handlers:
        return
    proxy = ProxyHandler(handlers)
    install_opener(build_opener(proxy))


def refresh():

    parser = RSSParser()
    content, resp = get('https://news.ycombinator.com/rss')
    parser.feed(content)
    # auto_proxy()

    with connect(cfg):
        create_tables()

        # Collect linked pages content
        in_db = set(l for l, in View('feed_item', ['link']).read())
        for item in parser.items:
            link = item['link']
            if link in in_db:
                continue
            else:
                logger.info('Load %s' % link)
                text = TextParser.get_text(link)
                item['text'] = text
        # Update db
        View('feed_item', {
            'title': 'title',
            'link': 'link',
            'pubdate': 'pubdate',
            'description': 'description',
            'text': 'text',
            'extra': 'extra',
            # TODO add feed.link
        }).write(parser.items)


def list_items(args):
    with connect(cfg):
        view = View('feed_item', ['title'])
        res = view.read(order=('pubdate', 'desc'), limit=args.limit)
        for pos, (title,) in enumerate(res):
            print('%s | %s' % (pos, title))

def read_item(args):
    if len(args.action) > 1:
        offset = args.action[1]
    else:
        offset = 0

    with connect(cfg):
        view = View('feed_item', ['title', 'text'])
        res = view.read(order=('pubdate', 'desc'), limit=1, offset=offset)
        title, text = res.one()
        print(title)
        print('-' * len(title))
        print(text)

if __name__ == '__main__':
    # url = 'https://medium.com/@iantien/top-takeaways-from-andy-grove-s-high-output-management-2e0ecfb1ea63'
    # url = 'http://firstround.com/review/hypergrowth-and-the-law-of-startup-physics'
    # content, resp = get(url)
    # print(content)

    import argparse
    parser = argparse.ArgumentParser(description='ReceSS')
    parser.add_argument('action', help='info, read, refresh', nargs='+')
    parser.add_argument('-v', '--verbose', action='count',
                        help='Increase verbosity')
    parser.add_argument('-l', '--limit', type=int, default=10,
                        help='Number of results')

    args = parser.parse_args()
    if args.verbose:
        logger.setLevel('DEBUG' if args.verbose > 1 else 'INFO')
        if args.verbose > 2:
            from tanker import logger as tanker_logger
            tanker_logger.setLevel('DEBUG')

    action = args.action[0]
    if action == 'refresh':
        refresh()
    elif action == 'list':
        list_items(args)
    elif action == 'read':
        read_item(args)
    else:
        exit('Action "%s" not supported' % action)
