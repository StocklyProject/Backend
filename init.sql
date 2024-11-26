CREATE DATABASE IF NOT EXISTS stockly;
USE stockly;

CREATE TABLE IF NOT EXISTS stock (
    id INT AUTO_INCREMENT PRIMARY KEY,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    is_deleted TINYINT DEFAULT 0,
    symbol VARCHAR(100),
    high INT,
    low INT,
    volume BIGINT,
    date DATETIME,
    open INT,
    close INT,
    rate DOUBLE,
    rate_price INT,
    trading_value DECIMAL(20, 2),
    is_daily BOOLEAN DEFAULT TRUE
);

ALTER TABLE stock
ADD UNIQUE KEY unique_symbol_date (symbol, date);

CREATE TABLE IF NOT EXISTS user (
  id INT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(100) NOT NULL,
  password VARCHAR(100) NOT NULL,
  name VARCHAR(100) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  is_deleted BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS company (
  id INT AUTO_INCREMENT PRIMARY KEY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  is_deleted TINYINT(1) DEFAULT 0,
  name VARCHAR(100) NOT NULL,
  symbol VARCHAR(100) NOT NULL
);

INSERT INTO company (name, symbol) VALUES
('삼성전자', '005930');
-- ('LG', '003550'),
-- ('SK하이닉스', '000660'),
-- ('삼성바이오로직스','207940'),
-- ('기아', '000270');