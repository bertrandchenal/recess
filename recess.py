#! /usr/bin/env python

from hashlib import md5
from itertools import chain
import os
import textwrap

from pyroaring import BitMap

from utils import (get, TextParser, RSSParser, logger, normalize, CachedMap,
                   LogMap)


class DB:

    def __init__(self, path):
        # page containes the actual payload
        self.page = LogMap(os.path.join(path, 'page'))
        # pageset contains sets of pages. It can be pages with the same
        # keyword or pages with the same url (but content has changed
        # over time)
        self.pageset = LogMap(os.path.join(path, 'pageset'))
        self.link = CachedMap(os.path.join(path, 'link'))
        self.word = CachedMap(os.path.join(path, 'word'))

    def update_word_index(self, fragments, doc_id):
        # Transform doc_id into bitset
        print(doc_id)
        words = list(chain.from_iterable((f.split() for f in fragments)))
        words = (normalize(m).lower() for m in set(words))
        words = set(w for w in words if len(w) > 1)
        for w in sorted(words):
            pageset_id = self.word.get(w)
            print(w, pageset_id)
            new_ps_id = self.update_pageset(doc_id, pageset_id)
            self.word[w] = new_ps_id

    def update_pageset(self, doc_id, pageset_id=None):
        if pageset_id is None:
            bm = BitMap()
        else:
            # pageset contains content-addressed bitmaps
            # TODO will fail on non-yet-flushed data
            bm_bytes = self.pageset.read_at(pageset_id)
            bm = BitMap.deserialize(bm_bytes)
        bm.add(doc_id)
        bm_bytes = bm.serialize()
        new_checksum = md5(bm_bytes).hexdigest()
        self.pageset[new_checksum] = bm_bytes # IDEA prefix payload with checksum
        return len(self.pageset) - 1

    def insert(self, link, fragments):
        if link in self.link:
            return
        link = link.strip()
        payload = link.encode() + b'\n' + ''.join(fragments).encode()
        key = md5(payload).hexdigest()
        self.page[key] = payload
        page_idx = len(self.page) - 1
        self.link[link] = page_idx
        self.update_word_index(fragments, page_idx)
        self.flush()

    def flush(self):
        self.pageset.flush()
        self.page.flush()
        self.word.flush()
        self.link.flush()

    def complete(self, word):
        for value, _ in self.word.search(word, 2):
            yield value

    def search(self, words):
        for word in words:
            for _, ps_id in self.word.search(word, 0):
                bm = self.pageset.read_at(ps_id)
                for offset in BitMap.deserialize(bm):
                    # yield self.page.read_at(offset, 1).decode()
                    content = self.page.read_at(offset)[:500].decode()
                    url, text = content.split('\n', 1)
                    yield '\n' +url
                    yield textwrap.fill(text)
    def compact(self):
        # TODO loop on all values of self.link and self.word and
        # re-generate a fresh self.pageset
        pass


def crawl(db, start_url):
    content, resp = get(start_url)
    content_type = resp.headers.get('Content-Type').split(';', 1)[0]
    if content_type == 'application/rss+xml':
        parser = RSSParser()
        parser.feed(content)
    else:
        logger.error(f'Content type {content_type} not supported for crawl')
        return

    # Collect linked pages content
    for item in parser.items:
        for link in (item['link'], item['extra']['comments']):
            logger.info(f'CRAWL {link}' )
            if link in db.link:
                continue
            else:
                logger.info('Load %s' % link)
                fragments = TextParser.get_text(link)
                if fragments is None:
                    continue
                db.insert(link, list(fragments))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='ReceSS')
    parser.add_argument('action', help='list, read, refresh', nargs='+')
    parser.add_argument('-v', '--verbose', action='count',
                        help='Increase verbosity')
    parser.add_argument('-l', '--limit', type=int, default=10,
                        help='Number of results')
    parser.add_argument('-u', '--url',
                        default='https://news.ycombinator.com/rss',
                        help='Starting page')

    args = parser.parse_args()
    if args.verbose:
        logger.setLevel('DEBUG' if args.verbose > 1 else 'INFO')

    db = DB('db')
    action, *extra = args.action
    if action == 'complete':
        for item in extra:
            for suggestion in db.complete(item):
                print(item, suggestion)
    elif action == 'search':
        for doc in db.search(extra):
            print(doc)
    elif action == 'crawl':
        crawl(db, args.url)
    elif action == 'insert':
        fragments = TextParser.get_text(args.url)
        db.insert(args.url, list(fragments))
    else:
        exit('Action "%s" not supported' % action)
