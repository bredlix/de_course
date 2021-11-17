--1=====================================================================================
select c.name AS cat
      ,COUNT(a.film_id) AS f_cnt
FROM film AS a
JOIN film_category AS b ON b.film_id=a.film_id
JOIN category AS c ON c.category_id=b.category_id
group BY 1
order BY 2 DESC;


--2=====================================================================================
SELECT CONCAT(e.first_name,' ',e.last_name) AS full_name
      ,count(DISTINCT a.rental_id) AS rent_cnt
FROM rental AS a
JOIN inventory AS b ON b.inventory_id=a.inventory_id
JOIN film AS c ON c.film_id=b.film_id
JOIN film_actor AS d ON d.film_id=c.film_id
JOIN actor AS e ON e.actor_id=d.actor_id
group BY 1
order by 2 DESC
LIMIT 10


--3=====================================================================================
SELECT f.name
      ,SUM(a.amount) AS revenue
FROM payment AS a
JOIN rental AS b ON b.rental_id=a.rental_id
JOIN inventory AS c ON c.inventory_id=b.inventory_id
JOIN film AS d ON d.film_id=c.film_id
JOIN film_category AS e ON e.film_id=d.film_id
JOIN category AS f ON f.category_id=e.category_id
group BY 1
order BY 2 DESC
limit 1;


--4=====================================================================================
SELECT a.title
FROM film AS a
left JOIN inventory i ON a.film_id = i.film_id
WHERE 1=1
  AND i.film_id is null;


--5=====================================================================================
SELECT a.full_name
      ,a.f_cnt
FROM(
    SELECT COUNT(DISTINCT a.title) AS f_cnt
          ,CONCAT(e.first_name,' ',e.last_name) AS full_name
          ,RANK() OVER(order BY COUNT(DISTINCT a.title) DESC) AS r
    FROM film AS a
    JOIN film_category AS b ON b.film_id=a.film_id
    JOIN category AS c ON c.category_id=b.category_id
    JOIN film_actor AS d ON d.film_id=a.film_id
    join actor AS e ON e.actor_id=d.actor_id
    WHERE 1=1
      AND c.name = 'Children'
    group BY 2
    order BY 1 DESC) AS a
WHERE 1=1
  and r <=3;


--6=====================================================================================
select c.city
      ,COUNT(CASE WHEN a.active=1 then a.customer_id ELSE NULL end) AS cnt_active
      ,COUNT(CASE WHEN a.active=0 then a.customer_id ELSE NULL end) AS cnt_inactive
  from customer AS a
JOIN address AS b ON b.address_id=a.address_id
JOIN city AS c ON c.city_id=b.city_id
group BY 1
order BY 3 desc;


--7=====================================================================================
SELECT a.city_group
      ,name AS Category
      ,rent_hours
FROM(
      SELECT e.name
            ,CASE WHEN h.city ILIKE 'a%' THEN 'start_w_A'
                  WHEN h.city LIKE '%-%' THEN 'contains_-'
                  ELSE 'T_T' end AS city_group
            ,SUM(date_part('day',return_date - rental_date)* 24 + date_part('hour',return_date - rental_date)) AS rent_hours
            ,ROW_NUMBER() OVER (partition BY CASE WHEN h.city ILIKE 'a%' THEN 'start_w_A'
                                                  WHEN h.city LIKE '%-%' THEN 'contains_-'
                                                  ELSE 'T_T' END  
                                order BY SUM(date_part('day',return_date - rental_date)* 24 + date_part('hour',return_date - rental_date)) desc) AS rn
      FROM rental AS a
      JOIN inventory AS b ON b.inventory_id=a.inventory_id
      JOIN film AS c ON c.film_id=b.film_id
      JOIN film_category AS d ON d.film_id=c.film_id
      JOIN category AS e ON e.category_id=d.category_id
      JOIN customer AS f ON f.customer_id=a.customer_id
      JOIN address AS g ON g.address_id=f.address_id
      JOIN city AS h ON h.city_id=g.city_id
      WHERE 1=1
        AND h.city ILIKE 'a%' OR h.city LIKE '%-%'
      GROUP BY 1,2
      order BY 3 desc, 2) AS a
WHERE 1=1
AND rn=1;
