from collections import defaultdict, OrderedDict
from html.parser import HTMLParser
from itertools import groupby
from urllib.request import (urlopen, Request, ProxyHandler, install_opener,
                            build_opener)
import gzip
import io
import logging
import os
import re
import unicodedata
import urllib

from dateutil import parser as dateutil_parser
from rust_fst import Map

fmt = '%(levelname)s:%(asctime).19s: %(message)s'
logging.basicConfig(format=fmt)
logger = logging.getLogger('recess')
logger.setLevel('WARN')

def get(url):
    ua = 'curl 7.16.1 (i386-portbld-freebsd6.2) libcurl/7.16.1 OpenSSL/0.9.7m zlib/1.2.3'
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
    encoding = None
    content_type = resp.getheader('Content-Type', '')
    token = 'charset='
    if token in content_type:
        encoding = content_type.rsplit(token)[1]
    return content.decode(encoding or 'utf-8', errors='replace'), resp

# class FSMap:
#     '''
#     Expose folder as a python map. FSMap hashes a key to xx/key, with
#     xx being the two first letters of the md5 sum of the key.
#     '''
#     def __init__(self, root):
#         self.root = root
#     def _path(self, key):
#         h = hashlib.md5(key.encode()).hexdigest()
#         return os.path.join(self.root, h[:2])
#     def __setitem__(self, key, value):
#         with open(self._path(key), 'w') as fh:
#             fh.write(value)
#     def __getitem__(self, key):
#         with open(self._path(key), 'w') as fh:
#             fh.read()


class CachedMap:

    def __init__(self, path):
        self._path = path
        self._cache = {} # TODO rename into _write_cache
        self._fst = None

    @property
    def fst(self):
        if self._fst is None:
            if os.path.exists(self._path):
                self._fst = Map(self._path)
            else:
                self._fst = Map.from_iter([])
        return self._fst

    def __setitem__(self, key, value):
        assert isinstance(value, int)
        self._cache[key] = value

    def __getitem__(self, key):
        try:
            return self._cache[key]
        except KeyError:
            pass
        _, value = next(self.fst.search(key, max_dist=0), (None, None))
        if value is None:
            raise KeyError(f"Key '{key}' not in CachedMap")
        return value

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        if key in self._cache:
            return True
        return key in self.fst

    def search(self, key, max_dist=0):
        return self.fst.search(key, max_dist=max_dist)

    def flush(self):
        tmp_path = f'{self._path}-tmp'
        new_fst = Map.from_iter(sorted(self._cache.items()))
        with Map.build(tmp_path) as tmp_map:
            for k, vals in self.fst.union(new_fst):
                tmp_map.insert(k, max(v.value for v in vals))
        # Rename tmp file
        os.rename(tmp_path, self._path)


class LogMap:
    '''
    LogMap is based on n append-only log file and an fst Map that
    indexes the log file content.
    '''
    def __init__(self, root, compress=False):
        # TODO implement compression
        os.makedirs(root, exist_ok=True)
        self.fst_path = os.path.join(root, 'fst')
        self.log_path = os.path.join(root, 'log')
        self.idx_path = os.path.join(root, 'idx')
        self._idx = None
        self._fst = None
        self._log = None
        self._cache_size = 0
        self._log_cache = OrderedDict()
        self._fst_cache = {}

    @property
    def fst(self):
        if self._fst is None:
            if os.path.exists(self.fst_path):
                self._fst = Map(self.fst_path)
            else:
                self._fst = Map.from_iter([])
        return self._fst

    @property
    def log(self):
        if self._log is None:
            self._log = open(self.log_path, 'ba+')
        return self._log

    @property
    def idx(self):
        if self._idx is None:
            self._idx = open(self.idx_path, 'ba+')
        return self._idx

    def _get_fst_map(self):
        if os.path.exists(self.link_fst):
            return Map(self.link_fst)
        return Map.from_iter([])

    def __setitem__(self, key, value):
        assert isinstance(value, bytes)
        self._log_cache[key] = value
        self._cache_size += len(value)
        self._fst_cache[key] = self.tell()
        # XXX auto-flush if size grows to much

    def __getitem__(self, key):
        try:
            return self._log_cache[key]
        except KeyError:
            pass
        offset = self.fst[key]
        return self.read_at(offset)

    def read_at(self, offset):
        self.idx.seek(offset * 8)
        idx_row = self.idx.read(8)
        log_pos = int.from_bytes(idx_row[:4], 'big', signed=False)
        length = int.from_bytes(idx_row[4:], 'big', signed=False)
        self.log.seek(log_pos)
        value = self.log.read(length)

        # Seek back to end
        self._idx.seek(0, os.SEEK_END)
        self._log.seek(0, os.SEEK_END)
        return value

    def tell(self):
        return self.log.tell() + self._cache_size

    def __len__(self):
        return self.idx.tell() // 8 + len(self._log_cache)

    def __contains__(self, key):
        if key in self._log_cache:
            return True
        return key in self.fst

    def flush(self):
        # Write to log
        for value in self._log_cache.values():
            # concat current log offset and value len in idx
            offset = self.log.tell()
            idx_row = offset.to_bytes(4, 'big', signed=False)
            idx_row += len(value).to_bytes(4, 'big', signed=False)
            self.idx.write(idx_row)
            # Append payload
            self.log.write(value)

        # Update fst
        # TODO use same dict to store the tuple (value, offset)
        new_fst = Map.from_iter(sorted(self._fst_cache.items()))
        tmp_path = f'{self.fst_path}-tmp'
        with Map.build(tmp_path) as tmp_map:
            for k, vals in self.fst.union(new_fst):
                tmp_map.insert(k, max(v.value for v in vals))
        # Rename tmp file
        os.rename(tmp_path, self.fst_path)

        # Close file descriptors
        self.log.close()
        self.idx.close()

class Matcher:
    def __init__(self, keys):
        self.keys = set(keys)

    def ok(self, key):
        for candidate in self.keys:
            prefix = key[:len(candidate)]
            if prefix == candidate:
                return True
        return False


def collapse(items):
    for x, _ in groupby(items):
        yield x


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
        # We could in theory simply call pop, but some pages do not
        # like to close all their tags, so keep popping until we find
        # the correct tag
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
        leaf = self.stack[-1]
        if leaf.name in self.skip:
            return
        key = tuple(collapse(key))
        self.rows.append((key, content))


    def find_match(self, test_func, items):
        for pos, item in enumerate(items):
            if test_func(item):
                return pos

    def topN(self, n=2):
        scores = defaultdict(list)
        for k, content in self.rows:
            scores[k].append(len(content))
        board = [(sum(s)/len(s), k) for k, s in scores.items()]
        keep = set(k for s, k in  sorted(board)[-n:])
        matcher = Matcher(keep)
        # Identify first and last rows that match and yield everything
        # between the two
        first = self.find_match(matcher.ok, (k for k, _ in self.rows))
        last = self.find_match(matcher.ok, (k for k, _ in reversed(self.rows)))
        for k, content in self.rows[first:-last]:
            yield content

    @classmethod
    def get_text(cls, link):
        if os.path.exists(link):
            return open(link)
        try:
            content, resp = get(link)
        except (urllib.error.HTTPError, urllib.error.URLError):
            logger.info('Unable to load %s' % link)
            return None
        content_type = resp.headers.get('Content-Type')
        if not content_type.startswith('text/html'):
            logger.info('Unable to parse %s' % link)
            return None

        tp = TextParser()
        tp.feed(content)
        return tp.topN()


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
                    leaf.content = dateutil_parser.parse(leaf.content)
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

no_symbols_re = re.compile('[^a-zA-Z0-9\-]+')
def normalize(data):
    norm = unicodedata.normalize('NFKD', data)
    return no_symbols_re.sub('', norm)
