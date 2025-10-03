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
year INTEGER NOT NULL,
semester VARCHAR(12) NOT NULL
);



CREATE TABLE time_of_day (
time_of_day_id INTEGER PRIMARY KEY,
time_of_day TIME NOT NULL,
hours INTEGER NOT NULL,
minutes INTEGER NOT NULL,
seconds INTEGER NOT NULL
);

CREATE TABLE category (
category_id INTEGER PRIMARY KEY,
name VARCHAR(32)
);

CREATE TABLE product (
product_id iNTEGER PRIMARY KEY,
name VARCHAR(32),
price NUMERIC(10,2),
active VARCHAR(32) NOT NULL DEFAULT 'deactive',
deactivate_date INTEGER,
deactivate_time_of_day INTEGER,
alcohol_content_ml NUMERIC(10,2),
FOREIGN KEY (deactivate_date ) REFERENCES  date (date_id),
FOREIGN KEY (deactivate_time_of_day ) REFERENCES  time_of_day (time_of_day_id)
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
date INTEGER NOT NULL,
time_of_day INTEGER NOT NULL,
room_id INTEGER NOT NULL,
price INTEGER NOT NULL DEFAULT 0,
PRIMARY KEY(member_id, product_id, date,time_of_day, room_id),
FOREIGN KEY (member_id) REFERENCES member (member_id),
FOREIGN KEY (product_id) REFERENCES product (product_id),
FOREIGN KEY (date) REFERENCES date (date_id),
FOREIGN KEY (time_of_day) REFERENCES  time_of_day (time_of_day_id),
FOREIGN KEY (room_id) REFERENCES room (room_id)
);
