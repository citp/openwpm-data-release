from __future__ import division
import sys
import sqlite3
from time import time
from os.path import basename, sep
from subprocess import call
from util import get_crawl_dir, get_crawl_db_path


ALEXA_ARCHIVE_BASE_URL = "https://toplists.net.in.tum.de/archive/alexa/"


def read_alexa_csv(csv_path):
    alexa_ranks = {}
    for l in open(csv_path):
        rank, domain = l.rstrip().split(",")
        alexa_ranks[domain] = rank
    return alexa_ranks


class FixAlexaRanks(object):

    def __init__(self, crawl_dir):
        self.crawl_dir = get_crawl_dir(crawl_dir)
        self.crawl_name = basename(crawl_dir.rstrip(sep))
        self.crawl_db_path = get_crawl_db_path(self.crawl_dir)
        self.init_db()
        self.get_crawl_start_date()

    def init_db(self):
        self.db_conn = sqlite3.connect(self.crawl_db_path)
        self.db_conn.row_factory = sqlite3.Row

    def fix_alexa_ranks(self):
        self.add_new_alexa_rank_col()
        if self.crawl_year >= 2017:
            self.add_real_alexa_rank_to_site_visits()
        else:
            self.copy_old_ranks_to_new_ranks_col()

    def get_crawl_start_date(self):
        crawl_start_time = self.db_conn.execute("""SELECT start_time FROM crawl
                ORDER BY start_time ASC LIMIT 1;""").fetchone()[0]
        self.crawl_date_ymd = crawl_start_time.split()[0]
        self.crawl_year = int(self.crawl_date_ymd.split("-")[0])

    def download_alexa_ranks(self):
        alexa_csv_name = "alexa-top1m-%s.csv" % self.crawl_date_ymd
        alexa_xz_archive_name = "%s.xz" % alexa_csv_name
        alexa_xz_archive_url = ALEXA_ARCHIVE_BASE_URL + alexa_xz_archive_name
        call(["wget", alexa_xz_archive_url])
        call(["unxz", alexa_xz_archive_name])
        return alexa_csv_name

    def add_new_alexa_rank_col(self):
        # support to RENAME columns was very recently introduced (in sqlite3)
        # we run this code on systems where we installed fresh built sqlite3
        # (v3.25) from source
        call(["sqlite3", self.crawl_db_path,
              "ALTER TABLE site_visits RENAME COLUMN site_rank "
              "TO crawled_alexa_rank;"])
        # self.db_conn.execute("""ALTER TABLE site_visits RENAME COLUMN
        # site_rank TO crawled_alexa_rank;""")
        self.db_conn.execute("ALTER TABLE site_visits ADD alexa_rank INTEGER;")

    def copy_old_ranks_to_new_ranks_col(self):
        self.db_conn.execute("""UPDATE TABLE site_visits
            SET alexa_rank = crawled_alexa_rank;""")

    def add_real_alexa_rank_to_site_visits(self):
        real_visit_ranks = {}
        alexa_csv_name = self.download_alexa_ranks()
        real_alexa_ranks = read_alexa_csv(alexa_csv_name)
        for visit_id, site_url in self.db_conn.execute(
             "SELECT visit_id, site_url FROM site_visits"):
            site_address = site_url.replace("http://", "")
            real_visit_ranks[visit_id] = real_alexa_ranks.get(site_address,
                                                              "None")

        for visit_id, alexa_rank in real_visit_ranks.iteritems():
            self.db_conn.execute("UPDATE site_visits SET alexa_rank=? "
                                 "WHERE visit_id=?",
                                 (alexa_rank, visit_id))


if __name__ == '__main__':
    t0 = time()
    fix_ranks = FixAlexaRanks(sys.argv[1])
    fix_ranks.fix_alexa_ranks()
    print "Ranks were updated in %0.1f mins" % ((time() - t0) / 60)
