CREATE TABLE member (
member_id INTEGER PRIMARY KEY,
active VARCHAR(20) NOT NULL DEFAULT 'deactive',
year INTEGER NOT NULL,
gender VARCHAR(32),
spam VARCHAR(32) NOT NULL DEFAULT 'no spam'
);

CREATE TABLE room (
room_id INTEGER PRIMARY KEY,
name VARCHAR(32),
description VARCHAR(32)
);

CREATE TABLE date (
date_id INTEGER PRIMARY KEY,
date DATE NOT NULL,
day INT NOT NULL,
month VARCHAR(12) NOT NULL,
year INTEGER NOT NULL
);



CREATE TABLE time_of_day (
time_of_day_id INTEGER PRIMARY KEY,
time_of_day TIME NOT NULL,
seconds INTEGER NOT NULL,
minutes INTEGER NOT NULL,
hours INTEGER NOT NULL
);

CREATE TABLE category (
category_id INTEGER PRIMARY KEY,
name VARCHAR(32)
);

CREATE TABLE product (
product_id iNTEGER PRIMARY KEY,
name VARCHAR(32),
price NUMERIC(10,2),
active BOOLEAN NOT NULL DEFAULT FALSE,
deactivate_date INTEGER,
alkohol_content_ml NUMERIC(10,2),
version INTEGER  NOT NULL,
newest_version BOOLEAN NOT NULL DEFAULT FALSE,
FOREIGN KEY (deactivate_date ) REFERENCES  date (date_id)
);

CREATE TABLE product_category (
product_id INTEGER NOT NULL,
category_id INTEGER NOT NULL,
weight NUMERIC(10,2),
PRIMARY KEY(product_id, category_id),
FOREIGN KEY (product_id) REFERENCES product (product_id),
FOREIGN KEY ( category_id) REFERENCES category ( category_id)
);

CREATE TABLE sale (
member_id INTEGER NOT NULL,
product_id INTEGER NOT NULL,
date_id INTEGER NOT NULL,
room_id INTEGER NOT NULL,
PRIMARY KEY(member_id, product_id, date_id, room_id),
FOREIGN KEY (member_id) REFERENCES member (member_id),
FOREIGN KEY (product_id) REFERENCES product (product_id),
FOREIGN KEY (date_id) REFERENCES date (date_id),
FOREIGN KEY (room_id) REFERENCES room (room_id)
);
