-- 1. Nombre moyen de commandes par utilisateur, et quantité moyenne par commande
SELECT
  count(*) / count(DISTINCT user_uuid) AS avg_orders_per_user,
  round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;

-- 2. Première et dernière commande de chaque utilisateur, et nombre de jours entre les deux
SELECT
  u.username,
  min(o.date) AS first_order,
  max(o.date) AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY u.username
ORDER BY days_between DESC;

-- 3. Heure de la journée avec la plus grande quantité vendue
SELECT hour(date) AS hour, sum(quantity) AS quantity
FROM orders
GROUP BY hour
ORDER BY quantity DESC
LIMIT 1;

-- 4. Variation mensuelle (mois sur mois) de la quantité vendue par produit, en pourcentage
WITH monthly AS (
  SELECT strftime(date, '%Y-%m') AS month, product, sum(quantity) AS quantity
  FROM orders
  GROUP BY month, product
)
SELECT
  product,
  month,
  quantity,
  lag(quantity) OVER (PARTITION BY product ORDER BY month) AS prev_quantity,
  round(100.0 * (quantity - lag(quantity) OVER (PARTITION BY product ORDER BY month))
        / lag(quantity) OVER (PARTITION BY product ORDER BY month), 1) AS mom_pct
FROM monthly
ORDER BY product, month;

-- 5. Utilisateurs ayant commandé chaque produit au moins une fois
SELECT u.username, u.name, count(DISTINCT o.product) AS products
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;
