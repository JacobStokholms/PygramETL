import sqlite3
from pathlib import Path

import pygrametl

sale_conn = sqlite3.connect("sale.sqlite",
        detect_types=sqlite3.PARSE_DECLTYPES)

