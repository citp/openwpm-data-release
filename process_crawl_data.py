import sys
import sqlite3
import os
from time import time
from util import get_table_and_column_names, load_alexa_ranks,\
    copy_if_not_exists, get_crawl_dir, get_crawl_db_path
from os.path import join, isfile, basename, isdir, sep
from normalize_db import add_site_visits_table, add_alexa_rank_to_site_visits,\
    add_missing_columns_to_all_tables, rename_crawl_history_table
from db_schema import SITE_VISITS_TABLE, CRAWL_HISTORY_TABLE
from fix_alexa_ranks import FixAlexaRanks

ROOT_OUT_DIR = "/mnt/10tb4/census-release"
if not isdir(ROOT_OUT_DIR):
    ROOT_OUT_DIR = "/tmp/census-release"

OPENWPM_LOG_FILENAME = "openwpm.log"
CRONTAB_LOG_FILENAME = "crontab.log"
ALEXA_TOP1M_CSV_FILENAME = "top-1m.csv"
JAVASCRIPT_SRC_DIRNAME = "content.ldb"
DEFAULT_SQLITE_CACHE_SIZE_GB = 20

# We won't be adding missing columns after the public release in Nov 2018.
# Instead crawl databases will reflect the changes in OpenWPM schema
# to keep up with the updates.
ADD_MISSING_COLUMNS = False


class CrawlData(object):

    def __init__(self, crawl_dir, out_dir):
        self.openwpm_log_path = ""
        self.crontab_log_path = ""
        self.alexa_csv_path = ""
        self.crawl_dir = get_crawl_dir(crawl_dir)
        print "Crawl dir", self.crawl_dir
        self.crawl_name = basename(crawl_dir.rstrip(sep))
        self.init_out_dirs(out_dir)
        self.crawl_db_path = get_crawl_db_path(self.crawl_dir)
        print "Crawl DB path", self.crawl_db_path
        self.set_crawl_file_paths()
        self.check_js_src_code()
        self.db_conn = sqlite3.connect(self.crawl_db_path)
        self.db_conn.row_factory = sqlite3.Row
        self.optimize_db()

    def init_out_dirs(self, out_dir):
        self.db_schema_dir = join(out_dir, "db-schemas")
        self.log_files_dir = join(out_dir, "log-files")
        self.alexa_ranks_dir = join(out_dir, "alexa-ranks")

        for _dir in [self.db_schema_dir,
                     self.log_files_dir,
                     self.alexa_ranks_dir]:
            if not isdir(_dir):
                os.makedirs(_dir)

    def optimize_db(self, size_in_gb=DEFAULT_SQLITE_CACHE_SIZE_GB):
        """ Runs PRAGMA queries to make sqlite better """
        self.db_conn.execute("PRAGMA cache_size = -%i" % (size_in_gb * 10**6))
        # Store temp tables, indices in memory
        self.db_conn.execute("PRAGMA temp_store = 2")
        # self.db_conn.execute("PRAGMA synchronous = NORMAL;")
        self.db_conn.execute("PRAGMA synchronous = OFF;")
        # self.db_conn.execute("PRAGMA journal_mode = WAL;")
        self.db_conn.execute("PRAGMA journal_mode = OFF;")

    def vacuum_db(self):
        """."""
        print "Will vacuum the DB",
        t0 = time()
        self.db_conn.execute("VACUUM;")
        print "finished in", float(time() - t0) / 60, "mins"

    def check_js_src_code(self):
        js_sources_dir = join(self.crawl_dir, JAVASCRIPT_SRC_DIRNAME)
        self.has_js_src = isdir(js_sources_dir)

    def set_crawl_file_paths(self):
        openwpm_log_path = join(self.crawl_dir, OPENWPM_LOG_FILENAME)
        if isfile(openwpm_log_path):
            self.openwpm_log_path = openwpm_log_path

        crontab_log_path = join(self.crawl_dir, CRONTAB_LOG_FILENAME)
        if isfile(crontab_log_path):
            self.crontab_log_path = crontab_log_path

        alexa_csv_path = join(self.crawl_dir, ALEXA_TOP1M_CSV_FILENAME)
        if isfile(alexa_csv_path):
            self.alexa_csv_path = alexa_csv_path

        print "OpenWPM log", self.openwpm_log_path
        print "Crontab log", self.crontab_log_path
        print "Alexa CSV", self.alexa_csv_path

    def pre_process(self):
        print "Will pre_process", self.crawl_dir
        self.backup_crawl_files()
        self.dump_db_schema()
        self.normalize_db()
        self.fix_alexa_ranks()
        # self.vacuum_db()

    def fix_alexa_ranks(self):
        fix_ranks = FixAlexaRanks(self.crawl_dir)
        fix_ranks.fix_alexa_ranks()

    def normalize_db(self):
        db_schema_str = get_table_and_column_names(self.crawl_db_path)
        # Add site_visits table
        if SITE_VISITS_TABLE not in db_schema_str:
            print "Adding site_visits table"
            add_site_visits_table(self.db_conn)
        if CRAWL_HISTORY_TABLE not in db_schema_str:
            print "Renaming CrawlHistory table to crawl_history"
            rename_crawl_history_table(self.db_conn)
        # Add site ranks to site_visits table
        if "site_rank" not in db_schema_str:
            if self.alexa_csv_path:
                print "Adding Alexa ranks to the site_visits table"
                site_ranks = load_alexa_ranks(self.alexa_csv_path)
                add_alexa_rank_to_site_visits(self.db_conn, site_ranks)
            else:
                print "Missing Alexa ranks CSV, can't add ranks to site_visits"
        if ADD_MISSING_COLUMNS:
            add_missing_columns_to_all_tables(self.db_conn, db_schema_str)
        print "Will commit the changes"
        self.db_conn.commit()

    def dump_db_schema(self):
        self.db_schema_str = get_table_and_column_names(self.crawl_db_path)
        out_str = self.db_schema_str
        out_str += "\nJavascript-source %s\n" % int(self.has_js_src)
        out_fname = self.crawl_name + "-db_schema.txt"
        db_schema_path = join(self.db_schema_dir, out_fname)
        print "Writing DB schema to %s" % db_schema_path
        with open(db_schema_path, 'w') as out:
            out.write(out_str)

    def backup_crawl_files(self):
        log_prefix = self.crawl_name + "-"
        if self.openwpm_log_path:
            openwpm_log_dst = join(self.log_files_dir,
                                   log_prefix + OPENWPM_LOG_FILENAME)
            copy_if_not_exists(self.openwpm_log_path, openwpm_log_dst)

        if self.crontab_log_path:
            crontab_log_dst = join(self.log_files_dir,
                                   log_prefix + CRONTAB_LOG_FILENAME)
            copy_if_not_exists(self.crontab_log_path, crontab_log_dst)

        if self.alexa_csv_path:
            alexa_csv_dst = join(self.alexa_ranks_dir,
                                 log_prefix + ALEXA_TOP1M_CSV_FILENAME)
            copy_if_not_exists(self.alexa_csv_path, alexa_csv_dst)


if __name__ == '__main__':
    t0 = time()
    crawl_data = CrawlData(sys.argv[1], sys.argv[2])
    crawl_data.pre_process()
    print "Preprocess finished in %0.1f mins" % ((time() - t0) / 60)
