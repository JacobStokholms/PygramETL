import sqlite3
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource, TransformingSource
from pygrametl.tables import CachedDimension, FactTable
from unicodedata import category

# Opening of connections and creation of a ConnectionWrapper.

dw_string = "host='localhost' dbname='fklub' user='dbs' password='dbs'"
dw_conn = psycopg2.connect(dw_string)
dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=dw_conn)

category_file_handle = open('category.csv', 'r', 16384, "utf-8")
category_source = CSVSource(f=category_file_handle, delimiter=';')

room_file_handle = open('room.csv', 'r', 16384, "utf-8")
room_source = CSVSource(f=room_file_handle, delimiter=';')

member_file_handle = open('member.csv', 'r', 16384, "utf-8")
member_source = CSVSource(f=member_file_handle, delimiter=';')

product_file_handle = open('product.csv', 'r', 16384, "utf-8")
product_source = CSVSource(f=product_file_handle, delimiter=';')


category_dimension = CachedDimension(
        name='category',
        key='category_id',
        attributes=['name'])

room_dimension = CachedDimension(
        name='room',
        key='room_id',
        attributes=['name','description'])

member_dimension = CachedDimension(
        name='member',
        key='member_id',
        attributes=['active','year','gender','spam'])

product_dimension = CachedDimension(
        name='product',
        key='product_id',
        attributes=['name','price','active','deactivate_date','alcohol_content_ml','version','newest_version'])



dw_conn_wrapper.execute("TRUNCATE TABLE category RESTART IDENTITY CASCADE;")
dw_conn_wrapper.execute("TRUNCATE TABLE room RESTART IDENTITY CASCADE;")
dw_conn_wrapper.execute("TRUNCATE TABLE member RESTART IDENTITY CASCADE;")
dw_conn_wrapper.execute("TRUNCATE TABLE product RESTART IDENTITY CASCADE;")
dw_conn_wrapper.commit()

def map_member(row):
    # normalize helper
    def is_t(val):
        return str(val).strip().lower() == 't'

    row['active'] = 'active' if is_t(row['active']) else 'deactive'
    row['spam'] = 'want spam' if is_t(row['want_spam']) else 'no spam'
    g = str(row.get('gender', '')).strip().lower()
    row['gender'] = {'m': 'male', 'f': 'female'}.get(g, 'unknown')

    # remove attributes you don't need downstream
    row.pop('undo_count', None)
    row.pop('balance', None)
    return row

def map_product(row):
    v = str(row.get('price', '')).strip()
    if v == '':
        row['price'] = None
    else:
        amt = Decimal(v) / Decimal(1000)
        row['price'] = amt.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)  # 2 decimals

    def is_t(val):
        return str(val).strip().lower() == 't'

    row['active'] = 'active' if is_t(row['active']) else 'deactive'
    row.pop('quantity', None)
    row.pop('start_date', None)

    row["deactivate_date"] = 1 // need to create date dimension and time dimension.

    row["version"] = 1
    row["newest_version"] = True

    return row

member_source_mapped = TransformingSource(member_source, map_member)
product_source_mapped = TransformingSource(product_source,map_product)

[category_dimension.insert(row) for row in category_source]
[room_dimension.insert(row) for row in room_source]
[member_dimension.insert(row) for row in member_source_mapped]
[product_dimension.insert(row) for row in product_source_mapped]

category_file_handle.close()
room_file_handle.close()
member_file_handle.close()
product_file_handle.close()

# The data warehouse connection is then ordered to commit and close
dw_conn_wrapper.commit()
dw_conn_wrapper.close()
