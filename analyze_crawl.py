from __future__ import division
import sys
import sqlite3
import os
from os.path import isfile, join
from collections import defaultdict
# from tqdm import tqdm
import util
from db_schema import (HTTP_REQUESTS_TABLE,
                       HTTP_RESPONSES_TABLE,
                       JAVASCRIPT_TABLE, OPENWPM_TABLES)
from util import dump_as_json, get_table_and_column_names


class CrawlDBAnalysis(object):

    def __init__(self, db_path, out_dir, crawl_name="unknown"):
        self.command_fail_rate = {}
        self.command_timeout_rate = {}
        if isfile(db_path):
            self.db_path = db_path
        else:
            raise ValueError("db_path is not a file")
        self.init_db()
        self.out_dir = out_dir
        self.crawl_name = crawl_name
        self.visit_id_site_urls = self.get_visit_id_site_url_mapping()
        self.sv_num_requests = defaultdict(int)
        self.sv_num_responses = defaultdict(int)
        self.sv_num_javascript = defaultdict(int)
        self.sv_num_third_parties = defaultdict(int)
        self.sv_third_parties = defaultdict(set)
        self.tp_to_publishers = defaultdict(set)
        self.rows_without_visit_id = 0

    def init_db(self):
        self.db_conn = sqlite3.connect(self.db_path)
        self.db_conn.row_factory = sqlite3.Row

    def run_all_streaming_analysis(self):
        self.run_streaming_analysis_for_table(HTTP_REQUESTS_TABLE)
        self.run_streaming_analysis_for_table(HTTP_RESPONSES_TABLE)
        self.run_streaming_analysis_for_table(JAVASCRIPT_TABLE)

    def get_visit_id_site_url_mapping(self):
        visit_id_site_urls = {}
        for visit_id, site_url in self.db_conn.execute(
                "SELECT visit_id, site_url FROM site_visits"):
            visit_id_site_urls[visit_id] = site_url
        print len(visit_id_site_urls), "mappings"
        print "Distinct site urls", len(set(visit_id_site_urls.values()))
        return visit_id_site_urls

    def run_streaming_analysis_for_table(self, table_name):
        current_visit_ids = {}
        processed = 0
        cols_to_select = ["visit_id", "crawl_id"]
        if table_name == HTTP_REQUESTS_TABLE:
            cols_to_select.append("url")
            # check whether top_level_url is here
            # ultimately preprocesing will make sure all tables contain
            # top_level_url
            try:
                self.db_conn.execute("SELECT top_level_url FROM %s LIMIT 1" %
                                     table_name)
                cols_to_select.append("top_level_url")
            except Exception:
                pass

        query = "SELECT %s FROM %s" % (",".join(cols_to_select), table_name)
        for row in self.db_conn.execute(query):
            processed += 1
            visit_id = int(row["visit_id"])
            crawl_id = int(row["crawl_id"])
            if visit_id == -1:
                self.rows_without_visit_id += 1
                continue

            site_url = self.visit_id_site_urls[visit_id]
            if table_name == HTTP_REQUESTS_TABLE:
                # use top_level_url, otherwise fall back to top_url
                self.sv_num_requests[site_url] += 1
                top_url = None
                if "top_level_url" in row:
                    top_url = row["top_level_url"]
                if top_url is None:
                    top_url = self.visit_id_site_urls[visit_id]
                if top_url:
                    is_tp, req_ps1, _ = util.is_third_party(
                        row["url"], top_url)
                    if is_tp:
                        self.sv_third_parties[site_url].add(req_ps1)
                        self.sv_num_third_parties[site_url] = len(
                            self.sv_third_parties[site_url])
                        self.tp_to_publishers[req_ps1].add(site_url)
                else:
                    print "Warning, missing top_url", row

            elif table_name == HTTP_RESPONSES_TABLE:
                self.sv_num_responses[site_url] += 1
            elif table_name == JAVASCRIPT_TABLE:
                self.sv_num_javascript[site_url] += 1

            if crawl_id not in current_visit_ids:
                current_visit_ids[crawl_id] = visit_id
            # end of the data from the current visit
            elif visit_id > current_visit_ids[crawl_id]:
                # self.process_visit_data(current_visit_data[crawl_id])
                # if site_url in self.sv_third_parties:
                #    del self.sv_third_parties[site_url]
                current_visit_ids[crawl_id] = visit_id
            elif visit_id < current_visit_ids[crawl_id] and visit_id > 0:
                # raise Exception(
                #    "Out of order row! Curr: %s Row: %s Crawl id: %s" %
                #    (current_visit_ids[crawl_id], visit_id, crawl_id))
                print "Warning: Out of order row! Curr: %s Row: %s Crawl id: %s" % (current_visit_ids[crawl_id], visit_id, crawl_id)

        self.dump_crawl_data(table_name)

    def print_num_of_rows(self):
        print "Will print the number of rows"
        db_schema_str = get_table_and_column_names(self.db_path)
        for table_name in OPENWPM_TABLES:
            # TODO: search in table names instead of the db schema
            if table_name in db_schema_str:
                try:
                    num_rows = self.db_conn.execute(
                        "SELECT MAX(id) FROM %s" % table_name).fetchone()[0]
                except sqlite3.OperationalError:
                    num_rows = self.db_conn.execute(
                        "SELECT COUNT(*) FROM %s" % table_name).fetchone()[0]
                if num_rows is None:
                    num_rows = 0
                print "Total rows", table_name, num_rows

    def dump_crawl_data(self, table_name):
        if table_name == HTTP_REQUESTS_TABLE:
            self.dump_json(self.sv_num_requests, "sv_num_requests.json")
            self.dump_json(self.sv_num_third_parties,
                           "sv_num_third_parties.json")
            # self.dump_json(self.sv_third_parties, "sv_third_parties.json")
            tp_to_publishers = {tp: "\t".join(publishers) for (tp, publishers)
                                in self.tp_to_publishers.iteritems()}
            self.dump_json(tp_to_publishers, "tp_to_publishers.json")
        elif table_name == HTTP_RESPONSES_TABLE:
            self.dump_json(self.sv_num_responses, "sv_num_responses.json")
        elif table_name == JAVASCRIPT_TABLE:
            self.dump_json(self.sv_num_javascript, "sv_num_javascript.json")

    def dump_json(self, obj, out_file):
        dump_as_json(obj, join(self.out_dir, "%s_%s" % (self.crawl_name,
                                                        out_file)))

    def start_analysis(self):
        self.print_num_of_rows()
        self.check_crawl_history()
        self.run_all_streaming_analysis()

    def check_crawl_history(self):
        """Compute failure and timeout rates for crawl_history table."""
        command_counts = {}  # num. of total commands by type
        fails = {}  # num. of failed commands grouped by cmd type
        timeouts = {}  # num. of timeouts
        for row in self.db_conn.execute(
            """SELECT command, count(*)
                FROM crawl_history
                GROUP BY command;""").fetchall():
            command_counts[row["command"]] = row["count(*)"]
            print "crawl_history Totals", row["command"], row["count(*)"]

        for row in self.db_conn.execute(
            """SELECT command, count(*)
                FROM crawl_history
                WHERE bool_success = 0
                GROUP BY command;""").fetchall():
            fails[row["command"]] = row["count(*)"]
            print "crawl_history Fails", row["command"], row["count(*)"]

        for row in self.db_conn.execute(
            """SELECT command, count(*)
                FROM crawl_history
                WHERE bool_success = -1
                GROUP BY command;""").fetchall():
            timeouts[row["command"]] = row["count(*)"]
            print "crawl_history Timeouts", row["command"], row["count(*)"]

        for command in command_counts.keys():
            self.command_fail_rate[command] = (fails.get(command, 0) /
                                               command_counts[command])
            self.command_timeout_rate[command] = (timeouts.get(command, 0) /
                                                  command_counts[command])
            self.dump_json(self.command_fail_rate, "command_fail_rate.json")
            self.dump_json(self.command_timeout_rate,
                           "command_timeout_rate.json")


if __name__ == '__main__':
    crawl_db_check = CrawlDBAnalysis(sys.argv[1], os.getcwd())
    crawl_db_check.start_analysis()
