#################################################
#
# 1. Import Libraries
#
import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager



#################################################
#
# 2. Configs
#
DB_CONFIG = {
    "host":     os.getenv("ANALYTICS_DB_HOST", "localhost"),
    "port":     int(os.getenv("ANALYTICS_DB_PORT", "5433")),
    "user":     os.getenv("ANALYTICS_DB_USER", "analytics_user"),
    "password": os.getenv("ANALYTICS_DB_PASSWORD", "analytics_pass"),
    "dbname":   os.getenv("ANALYTICS_DB_NAME", "analytics_db"),
}


#################################################
#
# 3. Context manager handles db connection and cursor
#
@contextmanager
def get_db():

    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
    finally:
        conn.close()