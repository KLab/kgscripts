#!/usr/bin/env python
# coding: utf-8
import datetime as dt
import os, re
import sys
import time
import logging
from ConfigParser import SafeConfigParser
from optparse import OptionParser

try:
    import MySQLdb
except ImportError:
    try:
        import pymysql as MySQLdb
    except ImportError:
        print >>sys.stderr, "Please install MySQL-python or PyMySQL."
        sys.exit(1)


class NoValueConfigParser(SafeConfigParser):
    """
    ConfigParser accepts no value.
    """
    OPTCRE = re.compile(
        r'(?P<option>[^:=\s][^:=]*)'          # very permissive!
        r'\s*'                                # any number of space/tab,
        r'(?P<vi>[:=]?)\s*'                   # optionally followed by
                                              # separator (either : or
                                              # =), followed by any #
                                              # space/tab
        r'(?P<value>.*)$'                     # everything up to eol
        )


def connect(cnf):
    args = {}
    args['host'] = cnf.get('host', 'localhost')
    args['user'] = cnf.get('user', '')
    args['passwd'] = cnf.get('password', '')
    args['charset'] = cnf.get('default-character-set', 'utf8')
    if 'port' in cnf:
        args['port'] = int(cnf.get('port'))
    if 'db' in cnf:
        args['db'] = cnf['db']
    return MySQLdb.connect(**args)

def read_mycnf(extra_file=None, group_suffix=''):
    cnf_files = ['/etc/my.cnf']
    if extra_file is not None:
        if not os.path.isfile(extra_file):
            print >>sys.stderr, "[warn]", extra_file, "is not exists."
        else:
            cnf_files += [extra_file]
    cnf_files += ['~/.my.cnf']
    cnf_files = map(os.path.expanduser, cnf_files)

    parser = NoValueConfigParser()
    parser.read(cnf_files)

    cnf = dict(parser.items('client'))
    if group_suffix:
        cnf.update(parser.items('client' + group_suffix))
    return cnf


def build_option_parser(usage='%prog [options]'):
    parser = OptionParser(add_help_option=False, usage=usage)
    parser.add_option(
            '-e', '--defaults-extra-file', dest='extra_file',
            help="Read MySQL configuration from this file additionaly",
            )
    parser.add_option(
            '-s', '--defaults-group-suffix', dest='group_suffix',
            help="Read MySQL configuration from this section additionally",
            )
    parser.add_option('-u', '--user')
    parser.add_option('-p', '--password')
    parser.add_option('-h', '--host')
    parser.add_option('-?', '--help', action="store_true", help="show this message")
    return parser


def delete_old_log(con, tablename, days, blocksize=100, dry=False):
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
        if dry:
            time.sleep(0.05)
        else:
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
    parser = build_option_parser('%prog [options] db_name table_name_1 [table_name_2...]')
    parser.add_option('-d', '--days', type='int', default=30)
    parser.add_option('-b', '--blocksize', type='int', default=100)
    parser.add_option('--dry', action='store_const', const=1, default=0)
    parser.add_option('-v', '--verbose', action='store_const', const=1, default=0)

    opts, args = parser.parse_args()
    outfile = None

    if opts.help:
        parser.print_help()
        return

    if len(args) < 2:
        parser.error('args is required.')
        return

    try:
        cnf = read_mycnf(opts.extra_file, opts.group_suffix)
        if opts.user:
            cnf['user'] = opts.user
        if opts.password:
            cnf['password'] = opts.password
        if opts.host:
            cnf['host'] = opts.host
        cnf['db'] = args[0]
        con = connect(cnf)
    except Exception, e:
        parser.error(e)
        return

    loglevel = logging.DEBUG if opts.verbose else logging.INFO
    logging.basicConfig(level=loglevel,
                        format="%(asctime)s %(levelname)-7s: %(message)s",
                        )

    con.autocommit(True)
    for table in args[1:]:
        logging.info("Start deleting old logs from: %s", table)
        delete_old_log(con, table, opts.days, opts.blocksize, dry=opts.dry)

if __name__ == '__main__':
    main()
