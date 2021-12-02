#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import RedisClient


def main():
    def _db_size(a_db: int) -> None:
        size = RedisClient(a_db).size()
        print(f"DB (name={a_db}): {size}")

    _db_size(RedisClient.WIKI_SEARCH_DB)
    _db_size(RedisClient.WIKI_PAGE_DB)
    _db_size(RedisClient.WIKI_AUGMENTED_DB)


if __name__ == "__main__":
    import plac

    plac.call(main)
