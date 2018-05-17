from textwrap import wrap
from collections import OrderedDict, defaultdict
from html.parser import HTMLParser
from urllib import request
import urllib

from tanker import connect, View, yaml_load, create_tables
import dateutil

# # Add proxy support
# proxy = request.ProxyHandler({
#     'http': 'http://proxy.eib.electrabel.be:8080',
#     'https': 'http://proxy.eib.electrabel.be:8080',
# })
# request.install_opener(request.build_opener(proxy))

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
        print(key, el.content[:30])
        self.rows.append((key, el.content))

    def handle_starttag(self, tag, attrs):
        self.stack.append(Element(tag, attrs))

    def handle_endtag(self, tag):
        # Consume stack until a matching tag is found (caused by
        # dangling open tags)
        while True:
            leaf_name = self.stack and self.stack[-1].name
            if not leaf_name or tag == leaf_name:
                break
            print('SKIP', leaf_name)
            self.stack.pop()
        if not self.stack:
            return

        self.record()
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
            resp = request.urlopen(link)
        except urllib.error.HTTPError:
            return None
        content_type = resp.headers.get('Content-Type')
        if not content_type.startswith('text/html'):
            return None

        source = resp.read().decode('utf-8', 'ignore')
        tp = TextParser()
        tp.feed(source)
        return '\n'.join(tp.topN(3))


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

if __name__ == '__main__':
    # resp = request.urlopen('https://news.ycombinator.com/rss')
    # source = resp.read().decode('utf-8')
    # parser = RSSParser()
    # parser.feed(source)
    # min_pubdate = min(i['pubdate'] for i in parser.items)

    with connect(cfg):
        create_tables()

        # # Collect linked pages content
        # in_db = set(l for l, in View('feed_item', ['link']).read())
        # for item in parser.items:
        #     link = item['link']
        #     if link in in_db:
        #         continue
        #     else:
        #         print('Load %s' % link)
        #         text = TextParser.get_text(link)
        #         item['text'] = text
        # # Update db
        # View('feed_item', {
        #     'title': 'title',
        #     'link': 'link',
        #     'pubdate': 'pubdate',
        #     'description': 'description',
        #     'text': 'text',
        #     'extra': 'extra',
        #     # TODO add feed.link
        # }).write(parser.items)

        # for link, txt in View('feed_item', ['link', 'text']).read():
        #     print('\n\n', link)
        #     if not txt:
        #         continue
        #     for line in txt.splitlines():
        #         if not line.strip():
        #             continue
        #         print('\n\t' + '\n\t'.join(wrap(line)))

        text = TextParser.get_text('https://news.ycombinator.com/item?id=17093538')
        print(text)
