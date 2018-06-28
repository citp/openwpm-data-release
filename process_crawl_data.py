import sys
from util import CRAWL_DB_EXT, get_table_and_column_names
from os.path import join, isfile, basename, isdir
import glob
from shutil import copyfile

ROOT_OUT_DIR = "/mnt/10tb4/census-release"
if not isdir(ROOT_OUT_DIR):
    ROOT_OUT_DIR = "/tmp/census-release"

DB_SCHEMA_DIR = join(ROOT_OUT_DIR, "db-schemas")
LOG_FILES_DIR = join(ROOT_OUT_DIR, "log-files")
ALEXA_RANKS_DIR = join(ROOT_OUT_DIR, "alexa-ranks")
OPENWPM_LOG_FILENAME = "openwpm.log"
CRONTAB_LOG_FILENAME = "crontab.log"
ALEXA_TOP1M_CSV_FILENAME = "top-1m.csv"
JAVASCRIPT_SRC_DIRNAME = "content.ldb"


class CrawlData(object):

    def __init__(self, crawl_dir):
        self.crawl_dir = crawl_dir
        self.crawl_name = basename(crawl_dir)
        self.crawl_db_path = ""
        self.openwpm_log_path = ""
        self.crontab_log_path = ""
        self.alexa_csv_path = ""
        self.set_db_path()
        self.set_crawl_file_paths()
        self.check_js_src_code()

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

        print "Log paths", self.openwpm_log_path, self.crontab_log_path
        print "Alexa CSV", self.alexa_csv_path

    def set_db_path(self):
        sqlite_files = glob.glob(join(self.crawl_dir, "*" + CRAWL_DB_EXT))
        assert len(sqlite_files) == 1
        self.crawl_db_path = sqlite_files[0]
        print "Crawl DB path", self.crawl_db_path

    def process(self):
        print "Will process", self.crawl_dir
        self.backup_crawl_files()
        self.dump_db_schema()

    def dump_db_schema(self):
        db_schema_str = get_table_and_column_names(self.crawl_db_path)
        db_schema_str += "\nJavascript-source %s\n" % int(self.has_js_src)
        # out_fname = basename(db_path).replace(CRAWL_DB_EXT, DB_SCHEMA_SUFFIX)
        out_fname = self.crawl_name + "-db_schema.txt"
        db_schema_path = join(DB_SCHEMA_DIR, out_fname)
        print "Writing DB schema to %s" % db_schema_path
        with open(db_schema_path, 'w') as out:
            out.write(db_schema_str)

    def backup_crawl_files(self):
        log_prefix = self.crawl_name + "-"
        if self.openwpm_log_path:
            openwpm_log_dst = join(LOG_FILES_DIR,
                                   log_prefix + OPENWPM_LOG_FILENAME)
            print "Copying %s to %s" % (self.openwpm_log_path, openwpm_log_dst)
            copyfile(self.openwpm_log_path, openwpm_log_dst)

        if self.crontab_log_path:
            crontab_log_dst = join(LOG_FILES_DIR,
                                   log_prefix + CRONTAB_LOG_FILENAME)
            print "Copying %s to %s" % (self.crontab_log_path, crontab_log_dst)
            copyfile(self.crontab_log_path, crontab_log_dst)

        if self.alexa_csv_path:
            alexa_csv_dst = join(ALEXA_RANKS_DIR,
                                 log_prefix + ALEXA_TOP1M_CSV_FILENAME)
            print "Copying %s to %s" % (self.alexa_csv_path, alexa_csv_dst)
            copyfile(self.alexa_csv_path, alexa_csv_dst)


if __name__ == '__main__':
    crawl_data = CrawlData(sys.argv[1])
    crawl_data.process()
