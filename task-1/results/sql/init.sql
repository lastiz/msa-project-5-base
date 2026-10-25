CREATE SCHEMA shop;

CREATE TABLE shop.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    amount NUMERIC(12, 2) NOT NULL CHECK (amount >= 0)
);

INSERT INTO shop.orders (order_id, customer_id, amount) VALUES
    (101, 1, 1200.00),
    (102, 2, 850.00),
    (103, 1, 2300.00),
    (104, 3, 600.00),
    (105, 4, 1500.00);
