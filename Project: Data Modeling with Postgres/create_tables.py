import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def database_creation():
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    conn.close()    
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def tables_creation(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    cur, conn = database_creation()
    
    drop_tables(cur, conn)
    tables_creation(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()