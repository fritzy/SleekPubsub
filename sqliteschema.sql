CREATE TABLE affiliation(id INTEGER PRIMARY KEY, node_id INTEGER, jid TEXT, type VARCHAR(10));
CREATE TABLE item(id INTEGER PRIMARY KEY, node_id INTEGER, payload BLOB, time DATETIME, who TEXT);
CREATE TABLE node (id INTEGER PRIMARY KEY, name VARCHAR(255), type VARCHAR(100), config BLOB);
CREATE TABLE roster(id INTEGER PRIMARY KEY, jid TEXT UNIQUE, subto INTEGER, subfrom INTEGER, jidto TEXT);
CREATE TABLE subscription(id INTEGER PRIMARY KEY, node_id INTEGER, jid TEXT, type VARCHAR(10), config BLOB, subid VARCHAR(255), jidto TEXT);
CREATE TABLE permissions(id INTEGER PRIMARY KEY, jid TEXT, auth varchar(10));
