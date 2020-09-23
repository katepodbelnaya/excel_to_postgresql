#!/usr/bin/python
import psycopg2
from config import config
import pandas as pd
import numpy as np
import time
from progress_bar import progressBar

def sql_execute(conn, sql):
    """ Execute an sql query"""
    # create a cursor
    cur = conn.cursor()

    # execute an sql query
    cur.execute(sql)

    # close the communication with the PostgreSQL
    cur.close()

    #commit the current transaction
    conn.commit()

def excel_to_postgresql():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config(filename='database.ini', section='postgresql')

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

	# execute a statement

        # read sql parameters
        sql_params = config(filename='database.ini', section='importfilename')

        # create dataframe from excel file
        df = pd.read_excel(sql_params['file_name'])

        df = df.replace(np.nan, '')

        # count table rows
        n = len(df)

        # create table
        sql_execute(conn, sql_params['sql_create'])
        print('Table from the {} was created.'.format(sql_params['file_name']))

        # upload data from dataframe to databade table row by row
        for i in progressBar(list(range(n)), prefix = 'Progress:', suffix = 'Complete', length = 50):
            cur = conn.cursor()
            row = list(df.loc[i])
            for i in range(len(row)):
                # get rid of single apostrophes
                if isinstance(row[i], str) and "'" in row[i]:
                    row[i] = row[i].replace("'", "")

            # add row from the dataframe to the table
            sql_execute(conn, sql_params['sql_insert'].format(tuple(row)))
            time.sleep(0.1)


        print('{} rows from the file was uploaded to the table.'.format(n))

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    excel_to_postgresql()
