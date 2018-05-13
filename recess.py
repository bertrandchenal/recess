from collections import defaultdict
from urllib import request
from html.parser import HTMLParser

from tanker import connect, View, yaml_load, create_tables
import dateutil


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
    extra: jsonb
  key:
    - link
'''

cfg = {
    'db_uri': 'postgresql:///test',
    'schema': yaml_load(schema),
}


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
    Extract meaningfull text from a webpage by identifying elements
    path with the longest average content.
    '''

    def __init__(self):
        self.scores = defaultdict(list)
        self.stack = []
        self.skip = set(['script', 'noscript', 'svg', 'img', 'g', 'input',
                         'form', 'html', 'body', 'path'])
        super().__init__()

    def inspect(self):
        el = self.stack[-1]
        key = tuple(i.name for i in self.stack)
        self.scores[key].append(len(el.content))

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
        if leaf.name in self.skip:
            return
        leaf.content += content

    def scoreboard(self):
        board = [(sum(s)/len(s), p) for p, s in parser.scores.items()]
        for k, v in  sorted(board)[-3:]:
            print(k , v)


class RSSParser(HTMLParser):
    '''
    Parse RSS files
    '''
    # XXX we may need to use xml.sax to be able to extract cdata

    def __init__(self):
        self.stack = []
        self.feed_info = {}
        self.items = []
        self.item_info = {}
        super().__init__()

    def inspect(self):
        el = self.stack[-1]
        prefix = tuple(i.name for i in self.stack[:-1])
        leaf = self.stack[-1]
        if prefix == ('rss', 'channel'):
            if leaf.name == 'item':
                self.items.append(self.item_info)
                self.item_info = {}
            else:
                self.feed_info[leaf.name] = leaf.content
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

if __name__ == '__main__':
    resp = request.urlopen('https://news.ycombinator.com/rss')
    source = resp.read().decode('utf-8')

    parser = RSSParser()
    parser.feed(source)

    with connect(cfg):
        create_tables()
        View('feed_item', {
            'title': 'title',
            'link': 'link',
            'pubdate': 'pubdate',
            'description': 'description',
            'extra': 'extra',
            # TODO add feed.link
        }).write(parser.items)
