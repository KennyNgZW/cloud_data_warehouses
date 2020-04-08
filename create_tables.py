import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Function to drop tables to make sure the creation of dimension tables and fact tables is valid
    Parameters: Redshift cursor instance, Redshift connection instance
    Returns: None
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Function to create dimension tables and fact table in star schema
    Parameters: Redshift cursor instance, Redshift connection instance
    Returns: None
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Main function to load the configuration records in dwh.cfg file to make connection to the redshift cluster
    Also invokes functions to drop tables first to make sure the creation of tables are valid and create dimension and fact tables.
    Parameters: None
    Returns: None
    '''  
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()