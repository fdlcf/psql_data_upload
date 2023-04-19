
import psycopg2
import numpy as np
import psycopg2.extras as extras
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
import psycopg2
import pandas as pd
import sys

def connect_to_psql(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

def get_df(path, sheet_name):
    path = path
    df = pd.read_excel(path, sheet_name=sheet_name)
    #df = df.where(pd.notnull(df), None)
    df = df.replace(np.nan, None, regex=True)
    print(df)
    return df


def execute_values_with_brakets(conn, df, table, update_on_conflict=False, conflict_id=None):
    """ Using psycopg2.extras.execute_values() to insert the dataframe """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = '","'.join(list(df.columns))

    if update_on_conflict:
        excluded_cols = [f'EXCLUDED."{i}"' for i in list(df.columns)]
        excluded_cols = ','.join(excluded_cols)
        query = f'INSERT INTO {table}("{cols}") VALUES %s ON CONFLICT ({conflict_id}) DO UPDATE SET ("{cols}") = ({excluded_cols}); '
        print(query)
    else:
        query = f'INSERT INTO {table}("{cols}") VALUES %s'
        print(query)

    template = '(' + ','.join(['%s' for i in range(len(df.columns))]) + ')'

    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples, template=template)
        #extras.execute_values(cursor, query, tuples, template=template)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()


def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))

    print(cols)
    # SQL query to execute
    query = 'INSERT INTO %s(%s) VALUES "%%s"' % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()


def alter_table(conn, sql):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
