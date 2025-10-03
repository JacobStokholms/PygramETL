import sqlite3
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

from dateutil import parser
import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource, TransformingSource
from pygrametl.tables import CachedDimension, FactTable
from unicodedata import category
 # pip install python-dateutil
import pandas as pd
import calendar
import calendar

# Opening of connections and creation of a ConnectionWrapper.

dw_string = "host='localhost' dbname='fklub' port='5433' user='postgres' password='admin'"
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

product_categories_file_handle = open('product_categories.csv', 'r', 16384, "utf-8")
product_categories_source = CSVSource(f=product_categories_file_handle, delimiter=';')

sale_file_handle = open('sale.csv', 'r', 16384, "utf-8")
sale_source = CSVSource(f=sale_file_handle, delimiter=';')

category_dimension = CachedDimension(
        name='category',
        key='category_id',
        attributes=['name'],
        lookupatts=['category_id'])

room_dimension = CachedDimension(
        name='room',
        key='room_id',
        attributes=['name','description'],
        lookupatts=['room_id'])

member_dimension = CachedDimension(
        name='member',
        key='member_id',
        attributes=['active','year','gender','spam'],
        lookupatts=['member_id'])

product_dimension = CachedDimension(
        name='product',
        key='product_id',
        attributes=['name','price','active','deactivate_date','deactivate_time_of_day','alcohol_content_ml'],
        lookupatts=['product_id'])

date_dimension = CachedDimension(
        name='date',
        key='date_id',
        attributes=['date','day', 'month', 'year','semester'])

time_of_day_dimension = CachedDimension(
        name='time_of_day',
        key='time_of_day_id',
        attributes=['time_of_day','hours', 'minutes', 'seconds'])

product_category_bridge = FactTable(
        name='product_category',
        keyrefs=['product_id', 'category_id'],
        measures=['weight'])

fact_table = FactTable(
        name='fact_api',
        keyrefs=['member_id', 'product_id','date','time_of_day','room_id'],
        measures=['price'])

dw_conn_wrapper.execute("TRUNCATE TABLE category RESTART IDENTITY CASCADE;")
dw_conn_wrapper.execute("TRUNCATE TABLE room RESTART IDENTITY CASCADE;")
dw_conn_wrapper.execute("TRUNCATE TABLE member RESTART IDENTITY CASCADE;")
dw_conn_wrapper.execute("TRUNCATE TABLE product RESTART IDENTITY CASCADE;")
dw_conn_wrapper.execute("TRUNCATE TABLE product_category RESTART IDENTITY CASCADE;")
dw_conn_wrapper.execute("TRUNCATE TABLE sale RESTART IDENTITY CASCADE;")
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
        amt = Decimal(v) / Decimal(100)
        row['price'] = amt.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)  # 2 decimals

    def is_t(val):
        return str(val).strip().lower() == 't'

    row['active'] = 'active' if is_t(row['active']) else 'deactive'
    row.pop('quantity', None)
    row.pop('start_date', None)

    row['name'] = row.get('name', 'Unknown Product')

    #row["deactivate_date"] # splitted later into date dimension and time dimension.

    return row

def split_date_product(row):
    """Splits a date represented by a datetime into its three parts"""

    # Splitting of the date into parts
    raw = row['deactivate_date']
    # 1) Default null if no date is exist
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        row['deactivate_time_of_day'] = None
    else:
        # 2) Parse: accept datetime as-is, otherwise parse string
        if isinstance(raw, datetime):
            date = raw
        else:
            try:
                date = parser.isoparse(str(raw))  # strict-ish ISO (handles '+02')
            except Exception:
                # Optional fallback: try fuzzy parse for non-ISO inputs
                date = parser.parse(str(raw))
        row['date'] = date.isoformat()
        row['year'] = date.year
        row['month'] = date.strftime('%B')
        row['day'] = date.day
        row["semester"] = 'Efterår' if (date.month >= 9 or date.month == 1) else 'Forår'


def split_time_product(row):
    """Splits the time part of deactivate_date into useful fields."""
    raw = row.get('deactivate_date')

    # 1) Default null if no date is exist
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        row['deactivate_date'] = None
    else:
        # 2) Parse to datetime (accept datetime as-is)
        if isinstance(raw, datetime):
            dt = raw
        else:
            try:
                dt = parser.isoparse(str(raw))    # strict-ish ISO
            except Exception:
                dt = parser.parse(str(raw))       # fuzzy fallback

        # 3) Extract time fields
        row['time_of_day']   = dt.strftime('%H:%M:%S')  # VARCHAR
        row['hours']   = dt.hour
        row['minutes'] = dt.minute
        row['seconds'] = dt.second



def split_date_sale(row):
    """Splits a date represented by a datetime into its three parts"""

    # Splitting of the date into parts
    raw = row['timestamp']
    # 1) Default null if no date is exist
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        row['timestamp'] = None
    else:
        # 2) Parse: accept datetime as-is, otherwise parse string
        if isinstance(raw, datetime):
            date = raw
        else:
            try:
                date = parser.isoparse(str(raw))  # strict-ish ISO (handles '+02')
            except Exception:
                # Optional fallback: try fuzzy parse for non-ISO inputs
                date = parser.parse(str(raw))
        row['date'] = date.isoformat()
        row['year'] = date.year
        row['month'] = date.strftime('%B')
        row['day'] = date.day
        row["semester"] = 'Efterår' if (date.month >= 9 or date.month == 1) else 'Forår'


def split_time_sale(row):
    """Splits the time part of deactivate_date into useful fields."""
    raw = row.get('timestamp')

    # 1) Default null if no date is exist
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        row['timestamp'] = None
    else:
        # 2) Parse to datetime (accept datetime as-is)
        if isinstance(raw, datetime):
            dt = raw
        else:
            try:
                dt = parser.isoparse(str(raw))    # strict-ish ISO
            except Exception:
                dt = parser.parse(str(raw))       # fuzzy fallback

        # 3) Extract time fields
        row['time_of_day']   = dt.strftime('%H:%M:%S')  # VARCHAR
        row['hours']   = dt.hour
        row['minutes'] = dt.minute
        row['seconds'] = dt.second


member_source_mapped = TransformingSource(member_source, map_member)
[member_dimension.insert(row) for row in member_source_mapped]


[category_dimension.insert(row) for row in category_source]
[room_dimension.insert(row) for row in room_source]

for row in product_source:

    map_product(row)
    split_date_product(row)
    split_time_product(row)

    if row['deactivate_date'] != None:
        row['deactivate_date'] = date_dimension.ensure(row)
        row['deactivate_time_of_day'] = time_of_day_dimension.ensure(row)

    product_dimension.insert(row)
dw_conn_wrapper.commit()

#for row in product_categories_source:
 #   row['product_id']  = product_dimension.ensure(row)   # resolves/creates product
  #  row['category_id'] = category_dimension.ensure(row)  # resolves/creates category
   # row['weight'] = row.get('weight', 1)

   # product_category_bridge.insert(row)

for row in product_categories_source:
    pid = row['product_id']
    cid = row['category_id']

    # Ensure product exists in product_dimension
    product_row = {
        'product_id': pid,
        'name': row.get('name', 'Unknown'),
        'price': row.get('price', None),
        'active': row.get('active', 'deactive'),
        'deactivate_date': row.get('deactivate_date', None),
        'deactivate_time_of_day': row.get('deactivate_time_of_day', None),
        'alcohol_content_ml': row.get('alcohol_content_ml', None)
    }
    row['product_id'] = product_dimension.ensure(product_row)

    # Ensure category exists in category_dimension
    category_row = {
        'category_id': cid,
        'name': row.get('category_id')
    }
    row['category_id'] = category_dimension.ensure(category_row)

    # Set weight, default to 1
    weight = row.get('weight', 1)

    # Insert into the bridge table safely using ON CONFLICT
    dw_conn_wrapper.execute("""
        INSERT INTO product_category (product_id, category_id, weight)
        VALUES (%s, %s, %s)
        ON CONFLICT (product_id, category_id) DO NOTHING
    """, (row['product_id'], row['category_id'], weight))

for row in sale_source:
    split_date_sale(row)
    split_time_sale(row)

    row['product_id'] = product_dimension.lookup(row)
    row['member_id'] = member_dimension.lookup(row)
    row['room_id'] = room_dimension.lookup(row)

    if row['timestamp'] != None:
        row['date'] = date_dimension.ensure(row)
        row['time_of_day'] = time_of_day_dimension.ensure(row)

    if row['price'] == None:
        row['price'] = 0

    if (row['timestamp'] != None and row['member_id'] != None and row['product_id'] != None and row['room_id'] != None) :
        dw_conn_wrapper.execute("""
               INSERT INTO sale (member_id,product_id, date,time_of_day,room_id, price)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (member_id,product_id, date,time_of_day,room_id) DO NOTHING
           """, (row['member_id'],row['product_id'],row['date'],row['time_of_day'], row['room_id'], row['price']))

category_file_handle.close()
room_file_handle.close()
member_file_handle.close()
product_file_handle.close()
product_categories_file_handle.close()
sale_file_handle.close()

# The data warehouse connection is then ordered to commit and close
dw_conn_wrapper.commit()
dw_conn_wrapper.close()
