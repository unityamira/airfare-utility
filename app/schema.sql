DROP TABLE IF EXISTS codes;
DROP TABLE IF EXISTS aliases;

CREATE TABLE aliases(
  alias TEXT PRIMARY KEY,
  iata_code TEXT 
  FOREIGN KEY(iata_code) REFERENCES codes(iata_code)
);

CREATE TABLE codes(
    iata_code TEXT PRIMARY KEY, 
    latitude REAL NOT NULL,
    longitude REAL NOT NULL
)