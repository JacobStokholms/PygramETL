import sqlite3

import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import CachedDimension, FactTable
from unicodedata import category

# Opening of connections and creation of a ConnectionWrapper.

dw_string = "host='localhost' dbname='fklub' user='dbs' password='dbs'"
dw_conn = psycopg2.connect(dw_string)
dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=dw_conn)

room_file_handle = open('category.csv', 'r', 16384, "utf-8")
category_source = CSVSource(f=room_file_handle, delimiter=';')

room_file_handle = open('category.csv', 'r', 16384, "utf-8")
category_source = CSVSource(f=room_file_handle, delimiter=';')


category_dimension = CachedDimension(
        name='category',
        key='category_id',
        attributes=['name'])

room_dimension = CachedDimension(
        name='category',
        key='category_id',
        attributes=['name'])

[category_dimension.insert(row) for row in category_source]

room_file_handle.close()

# The data warehouse connection is then ordered to commit and close
dw_conn_wrapper.commit()
dw_conn_wrapper.close()
