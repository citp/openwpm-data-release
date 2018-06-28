import sqlite3
from os.path import basename, join

CRAWL_DB_EXT = ".sqlite"
DB_SCHEMA_SUFFIX = "_db_schema.txt"


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
