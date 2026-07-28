INSERT INTO products (name, price, stock)
SELECT 'Wireless Mouse', 25.99, 10
WHERE NOT EXISTS (SELECT 1 FROM products WHERE name = 'Wireless Mouse');

INSERT INTO products (name, price, stock)
SELECT 'Mechanical Keyboard', 89.50, 5
WHERE NOT EXISTS (SELECT 1 FROM products WHERE name = 'Mechanical Keyboard');
