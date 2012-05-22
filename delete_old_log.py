#!/usr/bin/env python
# coding: utf-8
import datetime as dt
import os
import sys
import time
import logging
from ConfigParser import SafeConfigParser
import optparse

try:
    import MySQLdb
except ImportError:
    try:
        import pymysql as MySQLdb
    except ImportError:
        print >>sys.stderr, "Please install MySQL-python or PyMySQL."
        sys.exit(1)


def connect(conf='~/.my.cnf', section='batch'):
    """
    connect to MySQL from conf file.
    """
    parser = SafeConfigParser()
    parser.read([os.path.expanduser(conf)])
    config = dict(parser.items(section))
    params = {}

    for n in ('host', 'user', 'db'):
        if n in config:
            params[n] = config[n]

    if 'port' in config:
        params['port'] = int(config['port'])

    if 'password' in config:
        params['passwd'] = config['password']

    return MySQLdb.connect(**params)


def delete_old_log(con, tablename, days, blocksize=1000):
    u"""ログテーブルから古いログを消す.

    MySQL への接続 *con* を利用し、 *tablename* で指定された
    テーブルから、 *days* で指定された日数よりも古いテーブルを
    削除する. (*days* が 0 の時は、昨日以前のログを消す)

    対象となるテーブルは、PK が整数の `id` というカラム名で
    autoincrement されていて、 `created_at` というカラム名で
    作成日時が記録されている必要がある.

    *blocksize* は一括で削除する行数の初期値. 内部で自動的に
    削除にかかった時間をもとに増やしたり減らしたりします.
    """
    date = dt.date.today() - dt.timedelta(days=days)
    logging.info("delete records befor %s", date)

    cur = con.cursor()
    cur.execute("SELECT min(id),max(id) from %s" % (tablename,))
    MIN, MAX = map(int, cur.fetchone())
    breaking = False

    s = MIN
    while s < MAX and blocksize > 0:
        cur = con.cursor()
        e = min(MAX, s + blocksize - 1)
        cur.execute("SELECT created_at from %s WHERE id=%s" % (tablename, e))
        d = cur.fetchone()[0]
        if d is None:
            raise ValueError("created_at is NULL: pk=%r" % (e,))
        if d.date() >= date:
            blocksize //= 2
            breaking = True
            continue

        logging.info("deleting %s-%s (%s)", s, e, d)
        q = "DELETE LOW_PRIORITY FROM %s WHERE `id`<=%d" % (tablename, e)
        logging.debug(q)
        t = time.time()
        cur.execute(q)
        t = time.time() - t
        if t < 0.01 and not breaking:
            blocksize = int(blocksize * 1.1) +1
            logging.info("increase blocksize to %d", blocksize)
        if t > 0.1:
            blocksize = int(blocksize * 0.8) +1
            logging.info("decrease blocksize to %d", blocksize)
        time.sleep(0.01+t*20)
        s = e+1


def main():
    parser = optparse.OptionParser("%prog [OPTIONS] tablename")
    parser.add_option('-d', '--days', type='int', default=30)
    parser.add_option('-s', '--section', type='string', action="store", default='batch')
    parser.add_option('-b', '--blocksize', type='int', default=500)
    parser.add_option('-v', '--verbose', action='store_const', const=1, default=0)

    opts, args = parser.parse_args()
    if not args:
        parser.error("Only one tablename should be specified.")

    loglevel = logging.DEBUG if opts.verbose else logging.INFO
    logging.basicConfig(level=loglevel,
                        format="%(asctime)s %(levelname)-7s: %(message)s",
                        )

    con = connect(section=opts.section)
    con.autocommit(True)
    for table in args:
        logging.info("Start deleting old logs from: %s", table)
        delete_old_log(con, table, opts.days)

if __name__ == '__main__':
    main()
