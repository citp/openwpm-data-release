# TODO:task and crawl have different, non-overlapping columns across versions.
# xpath, site_visits, crawl_history, http_redirects has one version only
# flash cookies, profile_cookies has page_url/visit_id difference
# content_policy, pages: no table


DB_SCHEMA_HTTP_REQUESTS = """
    CREATE TABLE IF NOT EXISTS http_requests(
        id INTEGER PRIMARY KEY,
        crawl_id INTEGER NOT NULL,
        visit_id INTEGER NOT NULL,
        url TEXT NOT NULL,
        top_level_url TEXT,
        method TEXT NOT NULL,
        referrer TEXT NOT NULL,
        headers TEXT NOT NULL,
        channel_id TEXT,
        is_XHR BOOLEAN,
        is_frame_load BOOLEAN,
        is_full_page BOOLEAN,
        is_third_party_channel BOOLEAN,
        is_third_party_window BOOLEAN,
        triggering_origin TEXT,
        loading_origin TEXT,
        loading_href TEXT,
        req_call_stack TEXT,
        content_policy_type INTEGER,
        post_body TEXT,
        time_stamp TEXT NOT NULL
    );
    """

DB_SCHEMA_HTTP_RESPONSES = """
    CREATE TABLE IF NOT EXISTS http_responses(
        id INTEGER PRIMARY KEY,
        crawl_id INTEGER NOT NULL,
        visit_id INTEGER NOT NULL,
        url TEXT NOT NULL,
        method TEXT NOT NULL,
        referrer TEXT NOT NULL,
        response_status INTEGER NOT NULL,
        response_status_text TEXT NOT NULL,
        is_cached BOOLEAN,
        headers TEXT NOT NULL,
        channel_id TEXT,
        location TEXT NOT NULL,
        time_stamp TEXT NOT NULL,
        content_hash TEXT
    );
    """

DB_SCHEMA_JAVASCRIPT = """
    CREATE TABLE IF NOT EXISTS javascript(
        id INTEGER PRIMARY KEY,
        crawl_id INTEGER,
        visit_id INTEGER,
        script_url TEXT,
        script_line TEXT,
        script_col TEXT,
        func_name TEXT,
        script_loc_eval TEXT,
        document_url TEXT,
        top_level_url TEXT,
        call_stack TEXT,
        symbol TEXT,
        operation TEXT,
        value TEXT,
        arguments TEXT,
        time_stamp TEXT
    );
    """

DB_SCHEMA_JAVASCRIPT_COOKIES = """
    CREATE TABLE IF NOT EXISTS javascript_cookies(
        id INTEGER PRIMARY KEY,
        crawl_id INTEGER,
        visit_id INTEGER,
        change TEXT,
        creationTime DATETIME,
        expiry DATETIME,
        is_http_only INTEGER,
        is_session INTEGER,
        last_accessed DATETIME,
        raw_host TEXT,
        expires INTEGER,
        host TEXT,
        is_domain INTEGER,
        is_secure INTEGER,
        name TEXT,
        path TEXT,
        policy INTEGER,
        status INTEGER,
        value TEXT
    );
    """

DB_SCHEMA_FLASH_COOKIES = """
    CREATE TABLE IF NOT EXISTS flash_cookies (
        id INTEGER PRIMARY KEY,
        crawl_id INTEGER NOT NULL,
        visit_id INTEGER NOT NULL,
        domain VARCHAR(500),
        filename VARCHAR(500),
        local_path VARCHAR(1000),
        key TEXT,
        content TEXT,
        FOREIGN KEY(crawl_id) REFERENCES crawl(id),
        FOREIGN KEY(visit_id) REFERENCES site_visits(id)
    );
    """

DB_SCHEMA_PROFILE_COOKIES = """
    CREATE TABLE IF NOT EXISTS profile_cookies (
        id INTEGER PRIMARY KEY,
        crawl_id INTEGER NOT NULL,
        visit_id INTEGER NOT NULL,
        baseDomain TEXT,
        name TEXT,
        value TEXT,
        host TEXT,
        path TEXT,
        expiry INTEGER,
        accessed INTEGER,
        creationTime INTEGER,
        isSecure INTEGER,
        isHttpOnly INTEGER,
        FOREIGN KEY(crawl_id) REFERENCES crawl(id),
        FOREIGN KEY(visit_id) REFERENCES site_visits(id)
    );
    """

HTTP_REQUESTS_TABLE = "http_requests"
HTTP_RESPONSES_TABLE = "http_responses"
JAVASCRIPT_TABLE = "javascript"
JAVASCRIPT_COOKIES_TABLE = "javascript_cookies"
SITE_VISITS_TABLE = "site_visits"
CRAWL_HISTORY_TABLE = "crawl_history"
CRAWL_TABLE = "crawl"
TASK_TABLE = "task"
HTTP_REQUESTS_PROXY_TABLE = "http_requests_proxy"
HTTP_RESPONSES_PROXY_TABLE = "http_responses_proxy"
PROFILE_COOKIES_TABLE = "profile_cookies"
FLASH_COOKIES_TABLE = "flash_cookies"
LOCALSTORAGE_TABLE = "localStorage"

TABLE_SCHEMAS = {HTTP_REQUESTS_TABLE: DB_SCHEMA_HTTP_REQUESTS,
                 HTTP_RESPONSES_TABLE: DB_SCHEMA_HTTP_RESPONSES,
                 JAVASCRIPT_TABLE: DB_SCHEMA_JAVASCRIPT,
                 JAVASCRIPT_COOKIES_TABLE: DB_SCHEMA_JAVASCRIPT_COOKIES,
                 FLASH_COOKIES_TABLE: DB_SCHEMA_FLASH_COOKIES,
                 PROFILE_COOKIES_TABLE: DB_SCHEMA_PROFILE_COOKIES,
                 }

OPENWPM_TABLES = [
    HTTP_REQUESTS_TABLE,
    HTTP_RESPONSES_TABLE,
    JAVASCRIPT_TABLE,
    JAVASCRIPT_COOKIES_TABLE,
    SITE_VISITS_TABLE,
    CRAWL_HISTORY_TABLE,
    CRAWL_TABLE,
    TASK_TABLE,
    HTTP_REQUESTS_PROXY_TABLE,
    HTTP_RESPONSES_PROXY_TABLE,
    PROFILE_COOKIES_TABLE,
    FLASH_COOKIES_TABLE,
    LOCALSTORAGE_TABLE
    ]
