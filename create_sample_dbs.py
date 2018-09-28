import os
import sys
import sqlite3
import subprocess
from util import get_column_names

MAX_VISITS_TO_COPY_TO_SAMPLE_DB = 1000


def get_table_names_from_db(cursor):
    table_names = []
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    for row in cursor.fetchall():
        if row[0] == "sqlite_sequence":
            continue
        table_names.append(row[0])
    return table_names


def create_empty_db_from_existing_db(in_db, out_db):
    schema = subprocess.check_output(["sqlite3", in_db, '.schema'])
    # filter `sqlite_sequence` table
    schema = ''.join([line for line in schema.split("\n")
                      if "sqlite_sequence" not in line])
    conn = sqlite3.connect(out_db)
    conn.executescript(schema)
    conn.commit()
    conn.close()


def copy_rows_to_sample_db(in_db, out_db,
                           max_visits_to_copy=MAX_VISITS_TO_COPY_TO_SAMPLE_DB):
    conn = sqlite3.connect(in_db)
    cursor = conn.cursor()
    cursor.execute('ATTACH DATABASE "%s" AS db_sample' % out_db)
    table_names = get_table_names_from_db(cursor)

    visit_id_condition = "WHERE visit_id <= %s" % max_visits_to_copy
    for table_name in table_names:
        column_names = get_column_names(table_name, cursor)
        if "visit_id" in column_names:
            condition = visit_id_condition
        else:
            condition = ""
        cursor.execute('INSERT INTO db_sample.%s SELECT * FROM %s %s' %
                       (table_name, table_name, condition))
    conn.commit()


# USAGE:
# python crawl_db_path sample_crawl_db_path
if __name__ == '__main__':
    in_db = sys.argv[1]
    out_db = sys.argv[2]
    create_empty_db_from_existing_db(in_db, out_db)
    copy_rows_to_sample_db(in_db, out_db)
    print "In, out DB sizes", os.path.getsize(in_db), os.path.getsize(out_db)
