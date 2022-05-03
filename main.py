import psycopg2
import sqlite3
from sqlite3 import Error

# from decimal import *
# import zlib


def create_db():
    print("Connecting to the database...\n")
    try:
        conn = psycopg2.connect(
            "dbname='test' user='postgres1' password='TonHai1111' host='localhost' "
        )
        cursor = conn.cursor()
        print("Connected!")
        query = "create table details(label integer, timestamp timestamp,\
                    x float8[], y float8[], precip float8[], sids int8[],\
                    sids_cover int8[]);"
        query += "create table general(label integer, time_start timestamp,\
                    time_end timestamp, total_prec float8,\
                    one_stare_cover int8 );"
        print("creating tables...")
        cursor.execute(query)
        print("Details and General tables have been created!\n")

    except (Exception, psycopg2.Error) as error:
        print("Error: Cannot read data from the database. Error: ", error)
    finally:
        if conn:
            cursor.close()
            conn.commit()
            conn.close()
            print("PostgreSQL connection is closed")
    return


def drop_db():
    print("Connecting to the database...\n")
    try:
        conn = psycopg2.connect(
            "dbname='test' user='postgres1' password='TonHai1111' host='localhost' "
        )
        cursor = conn.cursor()
        print("Connected!")
        query = "drop table details;"
        query += "drop table general;"
        print("dropping tables...")
        cursor.execute(query)
        print("Details and General tables have been dropped!\n")

    except (Exception, psycopg2.Error) as error:
        print("Error: Cannot read data from the database. Error: ", error)
    finally:
        if conn:
            cursor.close()
            conn.commit()
            conn.close()
            print("PostgreSQL connection is closed")
    return


def query_ps_db_test():
    print("Connecting to the database...\n")
    try:
        conn = psycopg2.connect(
            "dbname='test' user='postgres1' password='TonHai1111' host='localhost' "
        )
        cursor = conn.cursor()
        print("Connected!")
        query = "select * from address"
        print("Reading data from Address tables...")
        cursor.execute(query)
        print("Printing data ...!\n")
        data = cursor.fetchall()
        for row in data:
            print(row)

    except (Exception, psycopg2.Error) as error:
        print("Error: Cannot read data from the database. Error: ", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")
    return


def insert_ps_db(queries):
    print("Connecting to the database...\n")
    try:
        conn = psycopg2.connect(
            "dbname='test' user='postgres1' password='TonHai1111' host='localhost' "
        )
        cursor = conn.cursor()
        print("Connected!")
        count = 0
        print("Inserting data ...!\n")
        for query in queries:
            cursor.execute(query)
            count += 1
            if count >= 9:
                conn.commit()
                count = 0
        print("Done!")

    except (Exception, psycopg2.Error) as error:
        print("Error: Cannot read data from the database. Error: ", error)
    finally:
        if conn:
            cursor.close()
            conn.commit()
            conn.close()
            print("PostgreSQL connection is closed")
    return


def loadDataFromSQLite(db_file="featuredb_small.gpkg"):
    print("Connecting to the SQLite3 database...\n")
    conn = None
    queries = []
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        print("Connected!")
        query = "select label, timestamp, sids_s, cover_s, precip_s, x_s, y_s from featuredb"
        cursor.execute(query)
        data = cursor.fetchall()
        print("print data ...\n")

        """
        CREATE TABLE IF NOT EXISTS featuredb 
        ( fid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
        geom MULTIPOLYGON, ????
        label INTEGER, 
        timestamp DATETIME, 
        sids_s TEXT, 
        cover_s TEXT, 
        precip_s TEXT, 
        areas_s TEXT, ????
        x_s TEXT, 
        y_s TEXT);
        """
        for row in data:
            # print(row)
            queries.append(get_insert_query_db(row))
    except Error as e:
        print("Error: Cannot read data from the SQLite3 database. Error: ", e)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("SQLite3 connection is closed")

    return queries


def get_insert_query_db(row):
    """Insert a row into details table (postgresql)

    Args:
        row (tuple): a tuple of data getting from sqlite3 with following format
        (<int: label>,
        <timestamp: timestamp>,
        <text: sid_s>, #[sid1, sid2 ... sidn]
        <text: cover_s>, #[sid1, sid2 ... sidn]
        <text: precip_s>, #[prec1, prec2 ... precn]
        <text: x_s>, #[x1, x2 ... xn]
        <text: y_s>) #[y1, y2 ... yn]

        Mapping:
        label       -->     label
        timestamp   -->     timestamp
        sid_s       -->     sids
        cover_s     -->     sids_cover
        precip_s    -->     precip
        x_s         -->     x
        y_s         -->     y
    """
    if not row:
        return None
    # sids = convert_square_bracket(row[2])
    # sids_cover = convert_square_bracket(row[3])
    # precip = convert_square_bracket(row[4])
    # x = convert_square_bracket(row[5])
    # y = convert_square_bracket(row[6])
    query = f"insert into details(label, timestamp, sids, \
                sids_cover, precip, x, y) \
                values('{row[0]}', '{row[1]}', \
                '{convert_square_bracket(row[2])}', \
                '{convert_square_bracket(row[3])}', \
                '{convert_square_bracket(row[4])}', \
                '{convert_square_bracket(row[5])}', \
                '{convert_square_bracket(row[2])}')"
    return query


def convert_square_bracket(value):
    if not value:
        return None
    value = value.replace("[", "{").replace("]", "}")
    return value


if __name__ == "__main__":
    print("Start...\n")
    # create_db()  # Run just once
    ## drop_db()
    ## query_ps_db() # For testing
    queries = loadDataFromSQLite()
    ## with open("temp.txt", "w") as file:
    ##    for query in queries:
    ##        file.write(str(query))
    # insert_ps_db(queries) #already ingested
    print("Done")
