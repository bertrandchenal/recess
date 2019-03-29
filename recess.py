from itertools import chain
import math
import os

from rust_fst import Map
from utils import get, TextParser, RSSParser, logger, normalize


class DB:

    def __init__(self, path):
        self.link_fst = os.path.join(path, 'link.fst')
        self.link_tmp_fst = self.link_fst + '-tmp'
        self.word_fst = os.path.join(path, 'word.fst')
        self.word_tmp_fst = self.word_fst + '-tmp'
        self.doc_log = os.path.join(path, 'docs.log')
        self.doc_idx = os.path.join(path, 'docs.idx')

    def append_document(self, content):
        # Append content to log
        with open(self.doc_log, 'a') as fh:
            doc_pos = fh.tell()
            fh.write(content)
        # Append position of new content in idx file
        with open(self.doc_idx, 'ab') as fh:
            idx_pos = fh.tell()
            fh.write(doc_pos.to_bytes(8, 'big'))
        # Return document position
        return idx_pos // 8

    def get_link_map(self):
        if os.path.exists(self.link_fst):
            return Map(self.link_fst)
        return Map.from_iter([])

    def get_word_map(self):
        if os.path.exists(self.word_fst):
            return Map(self.word_fst)
        return Map.from_iter([])

    def update_word_fst(self, fragments, doc_id):
        # Transform doc_id into bitset
        print(doc_id)
        doc_id = 1 << doc_id # TODO add a new log containing the bitset!
        word_fst = self.get_word_map()
        words = list(chain.from_iterable((f.split() for f in fragments)))
        words = (normalize(m).lower() for m in set(words))
        words = set(w for w in words if len(w) > 1)
        doc_fst = Map.from_iter((w, doc_id) for w in sorted(words))

        # Save union in tmp file
        with Map.build(self.word_tmp_fst) as tmp_map:
            for k, vals in word_fst.union(doc_fst):
                if len(vals) == 1:
                    v = vals[0].value
                elif len(vals) == 2:
                    v = vals[0].value | vals[0].value
                else:
                    raise ValueError('Unexpected!')
                tmp_map.insert(k, v)

        # Rename tmp file
        os.rename(self.word_tmp_fst, self.word_fst)

    def append_link(self, link, doc_id):
        new_fst = Map.from_iter([(link, doc_id)])
        link_fst = self.get_link_map()

        # Save union in tmp file
        with Map.build(self.link_tmp_fst) as tmp_map:
            for k, vals in link_fst.union(new_fst):
                tmp_map.insert(k, vals[0].value)

        # Rename tmp file
        os.rename(self.link_tmp_fst, self.link_fst)

    def insert(self, link, fragments):
        if link in self.get_link_map():
            return
        doc_id = self.append_document(''.join(fragments))
        self.update_word_fst(fragments, doc_id)
        self.append_link(link, doc_id)

    def complete(self, word):
        m = self.get_word_map()
        for value, _ in m.search(word, 2):
            yield value

    def search(self, words):
        m = self.get_word_map()
        bitset = None
        for word in words:
            for _, doc_id in m.search(word, 0):
                if bitset == 0:
                    return
                elif bitset is None:
                    bitset = doc_id
                else:
                    bitset = doc_id & bitset

        while bitset:
            doc_id = int(math.log(bitset, 2))
            yield f'FOUND {doc_id}'
            bitset = bitset - (1 << doc_id)


def crawl(db, start_url):
    link_fst = db.get_link_map()

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
            if link in link_fst:
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
        if args.verbose > 2:
            from tanker import logger as tanker_logger
            tanker_logger.setLevel('DEBUG')


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
        frag = list(fragments)
        db.insert(args.url, list(fragments))
    else:
        exit('Action "%s" not supported' % action)
