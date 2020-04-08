import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, count_table_queries


def load_staging_tables(cur, conn):
    '''
    Function to load data to staging tables from S3 by executing queries from sql_queries
    Parameters: Redshift cursor instance, Redshift connection instance
    Returns: None
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Function to insert data into dimension and fact tables from staging tables
    Parameters: Redshift cursor instance, Redshift connection instance
    Returns: None
    '''
    for query in insert_table_queries:
        # print(query)
        cur.execute(query)
        conn.commit()



def count_tables_size(cur, conn):
    '''
    Function to count the size of the table and print the results in table format by calling print_result function
    Parameters: Redshift cursor instance, Redshift connection instance
    Returns: None
    '''
    for query in count_table_queries:
        cur.execute(query)
        results = cur.fetchall()
        print_result(results, cur)
        conn.commit()

def print_result(results, cursor):
    '''
    Function to print the result of the query after inserting data to tables in tablar form
    Parameters: result of query
    Returns: None
    '''
    widths = []
    columns = []
    tavnit = '|'
    separator = '+' 

    for cd in cursor.description:
        if cd[2] == None:
            cd2 = 0
        else:
            cd2 = cd[2]
        widths.append(max(cd2, len(cd[0])))
        columns.append(cd[0])

    for w in widths:
        tavnit += " %-"+"%ss |" % (w,)
        separator += '-'*w + '--+'

    print(separator)
    print(tavnit % tuple(columns))
    print(separator)
    for row in results:
        print(tavnit % row)
    print(separator)
    print('******************')
        
def main():
    '''
    Main function to load the configuration records in dwh.cfg file to make connection to the redshift cluster
    Also invokes functions to load data in S3 bucket to staging tables and from the staging tables to load data to dimension and fact tables.
    Parameters: None
    Returns: None
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print('staging tables are loaded')
    insert_tables(cur, conn)
    print('dimension and fact tables are inserted')
    count_tables_size(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()