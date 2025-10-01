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
        attributes=['name','price','active','deactivate_date','deactivate_time_of_day','alcohol_content_ml'])

date_dimension = CachedDimension(
        name='date',
        key='date_id',
        attributes=['date','day', 'month', 'year','semester'])

time_of_day_dimension = CachedDimension(
        name='time_of_day',
        key='time_of_day_id',
        attributes=['time_of_day','hours', 'minutes', 'seconds'])


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
        amt = Decimal(v) / Decimal(100)
        row['price'] = amt.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)  # 2 decimals

    def is_t(val):
        return str(val).strip().lower() == 't'

    row['active'] = 'active' if is_t(row['active']) else 'deactive'
    row.pop('quantity', None)
    row.pop('start_date', None)

    #row["deactivate_date"] = 1 # need to create date dimension and time dimension.

    return row

def split_date(row):
    """Splits a date represented by a datetime into its three parts"""

    # Splitting of the date into parts
    raw = row['deactivate_date']
    # 1) Handle null-ish values early
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        # Your table has NOT NULL columns, so either skip this row or set placeholders.
        #row.update({"date": None, "year": None, "month": None, "day": None, "semester": None})
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


def split_time(row):
    """Splits the time part of deactivate_date into useful fields."""
    raw = row.get('deactivate_date')

    # 1) Handle null-ish values
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


member_source_mapped = TransformingSource(member_source, map_member)
product_source_mapped = TransformingSource(product_source,map_product)

[category_dimension.insert(row) for row in category_source]
[room_dimension.insert(row) for row in room_source]
[member_dimension.insert(row) for row in member_source_mapped]
#[product_dimension.insert(row) for row in product_source_mapped]

for row in product_source:

    # Each row is passed to the date split function for splitting
    map_product(row)
    split_date(row)
    split_time(row)

    # Lookups are performed to find the key in each dimension for the fact
    # and if the data is not there, it is inserted from the sales row
    if row['deactivate_date'] != None:
        row['deactivate_date'] = date_dimension.ensure(row)
        row['deactivate_time_of_day'] = time_of_day_dimension.ensure(row)

    # The location dimension is pre-filled, so a missing row is an error
    #row['locationid'] = location_dimension.lookup(row)
    #if not row['locationid']:
    #    raise ValueError("city was not present in the location dimension")

    # The row can then be inserted into the fact table
    product_dimension.insert(row)


category_file_handle.close()
room_file_handle.close()
member_file_handle.close()
product_file_handle.close()

# The data warehouse connection is then ordered to commit and close
dw_conn_wrapper.commit()
dw_conn_wrapper.close()
