import sqlite3
from multiprocessing import Process
from tld import get_tld
import ipaddress
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse


CRAWL_DB_EXT = ".sqlite"
DB_SCHEMA_SUFFIX = "_db_schema.txt"


def load_alexa_ranks(alexa_csv_path):
    site_ranks = dict()
    for line in open(alexa_csv_path):
        parts = line.strip().split(',')
        site_ranks[parts[1]] = int(parts[0])
    return site_ranks


def get_column_names(table_name, cursor):
    """Return the column names for a table.

    Modified from https://stackoverflow.com/a/38854129
    """
    cursor.execute("SELECT * FROM %s" % table_name)
    return " ".join([member[0] for member in cursor.description])


def get_table_and_column_names(db_path):
    """Return the table and column names for a database.

    Modified from: https://stackoverflow.com/a/33100538
    """
    db_schema_str = ""
    db = sqlite3.connect(db_path)
    cursor = db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    for table_name in cursor.fetchall():
        table_name = table_name[0]
        db_schema_str += "%s %s\n" % (table_name, get_column_names(table_name,
                                                                   cursor))
    return db_schema_str


def start_worker_processes(worker_function, queue, num_workers=1):
    workers = []
    for _ in xrange(num_workers):
        worker_proc = Process(target=worker_function, args=(queue,))
        worker_proc.start()
        workers.append(worker_proc)
    return workers


def get_tld_or_host(url):
    try:
        return get_tld(url, fail_silently=False)
    except Exception:
        hostname = urlparse(url).hostname
        try:
            ipaddress.ip_address(hostname)
            return hostname
        except Exception:
            return None


def is_third_party(req_url, top_level_url):
    # TODO: when we have missing information we return False
    # meaning we think this is a first-party
    # let's make sure this doesn't have any strange side effects
    # We can also try returning `unknown`.
    if not top_level_url:
        return (None, "", "")

    site_ps1 = get_tld_or_host(top_level_url)
    if site_ps1 is None:
        return (None, "", "")

    req_ps1 = get_tld_or_host(req_url)
    if req_ps1 is None:
        # print url
        return (None, "", site_ps1)
    if (req_ps1 == site_ps1):
        return (False, req_ps1, site_ps1)

    return (True, req_ps1, site_ps1)
