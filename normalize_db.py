import time
import sqlite3

from db_schema import TABLE_SCHEMAS


TABLES_WITH_TOP_URL = ["http_requests", "http_responses",
                       "javascript", "javascript_cookies"]


def add_visit_id_col_to_tables(con):
    for table_name in TABLES_WITH_TOP_URL:
        try:
            con.execute("ALTER TABLE %s ADD COLUMN visit_id INTEGER"
                        % table_name)
        except sqlite3.OperationalError:
            pass


def create_site_visits_table(con):
    con.execute("""CREATE TABLE IF NOT EXISTS site_visits (
                visit_id INTEGER PRIMARY KEY,
                crawl_id INTEGER NOT NULL,
                site_url VARCHAR(500) NOT NULL,
                FOREIGN KEY(crawl_id) REFERENCES crawl(id));""")


def add_site_visits_table(con):
    site_visits = []
    cur = con.cursor()
    create_site_visits_table(cur)
    # TODO: file an issue for that
    # we have duplicate visits for some sites
    # this is due to restarted crawls
    # See http://alweeam.com.sa in 2016-01_spider_4 for an example
    # The following query causes
    # query = "select DISTINCT top_url, MAX(crawl_id) from http_requests"
    query = "SELECT top_url, MAX(crawl_id) FROM http_requests GROUP BY top_url"
    for visit_id, (top_url, crawl_id) in enumerate(cur.execute(query)):
        if not top_url:
            print "Warning: Empty top-url", top_url, crawl_id
        site_visits.append((visit_id, crawl_id, top_url))
    cur.executemany('INSERT INTO site_visits VALUES (?,?,?)', site_visits)


def add_alexa_rank_to_site_visits(con, site_ranks):
    visit_ranks = {}
    for visit_id, site_url in con.execute(
            "SELECT visit_id, site_url FROM site_visits"):
        site_address = site_url.replace("http://", "")
        try:
            visit_ranks[visit_id] = site_ranks[site_address]
        except Exception:
            print "Exception while adding Alexa ranks", site_address
    con.execute("ALTER TABLE site_visits ADD COLUMN site_rank INTEGER")
    for visit_id, site_rank in visit_ranks.iteritems():
        con.execute("UPDATE site_visits SET site_rank=? WHERE visit_id=?",
                    (site_rank, visit_id))


def add_missing_columns(con, table_name, db_schema_str):
    existing_columns = get_column_names_from_db_schema_str(table_name,
                                                           db_schema_str)
    col_to_replace = None
    if "top_url" in existing_columns:
        col_to_replace = "top_url"
        assert "visit_id" not in existing_columns
    elif "page_url" in existing_columns:
        col_to_replace = "page_url"
        assert "visit_id" not in existing_columns

    new_columns = get_column_names_from_create_query(
        TABLE_SCHEMAS[table_name])
    if new_columns == existing_columns:
        print "No missing columns to add"
        return
    print "Will add missing columns to %s: %s" % (table_name, set(
        new_columns).difference(set(existing_columns)))
    tmp_table_name = "_%s_old" % table_name
    con.execute("ALTER TABLE %s RENAME TO %s;" % (table_name, tmp_table_name))

    # create table with the most recent schema
    con.execute(TABLE_SCHEMAS[table_name])

    # only keep the columns that also appear in the new table schema
    common_columns = [column for column in existing_columns
                      if column in new_columns]
    if col_to_replace:
        # replace top_url and page_url columns with visit_id
        print "Will replace %s with visit_id" % col_to_replace
        t1_cols = ",".join(["%s.%s" % (tmp_table_name, col_name)
                            for col_name in existing_columns])
        # make sure we add visit_id data from the temp table
        common_columns.append("visit_id")

        sub_qry = """SELECT %s, site_visits.visit_id
                        FROM %s
                        INNER JOIN site_visits
                        ON %s.%s=site_visits.site_url""" % (
            t1_cols, tmp_table_name, tmp_table_name, col_to_replace)

        qry = "INSERT INTO %s (%s) SELECT %s FROM (%s)" % (
            table_name, ",".join(common_columns),
            ",".join(common_columns), sub_qry)
        print "Will execute %s" % qry
        con.execute(qry)
    else:
        con.execute("INSERT INTO %s (%s) SELECT %s FROM %s" % (
            table_name, ",".join(common_columns),
            ",".join(common_columns), tmp_table_name))
    con.execute("DROP TABLE %s" % tmp_table_name)
    con.execute("VACUUM;")
    con.commit()


def get_column_names_from_create_query(create_table_query):
    col_names = []
    for line in create_table_query.split("\n"):
        line = line.strip()
        if line.startswith("CREATE") or line.startswith(")") or not line:
            continue
        col_names.append(line.split()[0])
    return col_names


def get_column_names_from_db_schema_str(table_name, db_schema_str):
    for l in db_schema_str.split("\n"):
        db_table_name = l.split()[0]
        if db_table_name == table_name:
            return l.split()[1:]
    else:
        raise Exception("Cannot find table %s in the DB" % table_name)


def add_missing_columns_to_all_tables(con, db_schema_str):
    for table_name in TABLE_SCHEMAS.keys():
        # TODO: search in table names instead of the db schema
        if table_name in db_schema_str:
            t0 = time.time()
            add_missing_columns(con, table_name, db_schema_str)
            duration = time.time() - t0
            print "Took %s s to add missing columns to %s" % (duration,
                                                              table_name)


if __name__ == '__main__':
    pass
