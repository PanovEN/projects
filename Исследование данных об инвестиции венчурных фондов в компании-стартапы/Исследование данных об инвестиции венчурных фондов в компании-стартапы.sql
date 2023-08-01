-- Цель проекта: проанализировать данные о фондах и инвестициях и написать запросы к базе --
/* Необходимо составить 23 запроса к БД (PostgreSQL) на основе датасета Startup Investments с Kaggle (https://www.kaggle.com/justinas/startup-investments) */

/* 1. Запрос на рассчёт, сколько компаний закрылось. */

SELECT COUNT(company)
FROM company
WHERE STATUS LIKE '%closed%';

/* 2. Отображаю количество привлечённых средств для новостных компаний США. Использую данные из таблицы company.
Сортирую таблицу по убыванию значений в поле funding_total . */

SELECT SUM(funding_total) OVER(PARTITION BY name)
FROM company
WHERE category_code = 'news'
  AND country_code = 'USA'
ORDER BY funding_total DESC;

/* 3. Нахожу общую сумму сделок по покупке одних компаний другими в долларах.
Отбираю сделки, которые осуществлялись только за наличные с 2011 по 2013 год включительно. */

SELECT SUM(price_amount)    
FROM acquisition
WHERE term_code = 'cash'
  AND CAST(DATE_TRUNC('year', acquired_at) AS date) BETWEEN '2011-01-01' AND '2013-12-31';

/* 4. Отображаю имя, фамилию и названия аккаунтов людей в твиттере, у которых названия аккаунтов начинаются на 'Silver'. */

SELECT first_name, last_name, twitter_username
FROM people
WHERE twitter_username LIKE 'Silver%';

/* 5. Вывожу на экран всю информацию о людях, у которых названия аккаунтов в твиттере содержат подстроку 'money', а фамилия начинается на 'K'. */

SELECT *
FROM people
WHERE twitter_username LIKE '%money%'
  AND last_name LIKE 'K%';

/* 6. Для каждой страны вывожу общую сумму привлечённых инвестиций, которые получили компании, зарегистрированные в этой стране.
Страну, в которой зарегистрирована компания, определяю по коду страны. Сортирую данные по убыванию суммы. */ 

SELECT country_code,
       SUM(funding_total) AS investment
FROM company
GROUP BY country_code
ORDER BY investment DESC;

/* 7. Составляю таблицу, в которую войдёт дата проведения раунда, а также минимальное и максимальное значения суммы инвестиций, привлечённых в эту дату.
Оставляю в итоговой таблице только те записи, в которых минимальное значение суммы инвестиций не равно нулю и не равно максимальному значению. */

SELECT funded_at, MIN(raised_amount), MAX(raised_amount)
FROM funding_round
GROUP BY funded_at
HAVING MIN(raised_amount) != 0
   AND MIN(raised_amount) != MAX(raised_amount);

/* 8. Создаю поле с категориями:
Для фондов, которые инвестируют в 100 и более компаний, назначаю категорию high_activity.
Для фондов, которые инвестируют в 20 и более компаний до 100, назначаю категорию middle_activity.
Если количество инвестируемых компаний фонда не достигает 20, назначаю категорию low_activity.
Вывожу все поля таблицы fund и новое поле с категориями. */

SELECT *,
       CASE
           WHEN invested_companies >= 100 THEN 'high_activity'
           WHEN invested_companies >= 20 AND invested_companies < 100 THEN 'middle_activity'
           WHEN invested_companies < 20 THEN 'low_activity'
       END
FROM fund;

/* 9. Для каждой из категорий, назначенных в предыдущем задании, считаю округлённое до ближайшего целого числа среднее количество инвестиционных раундов, в которых фонд принимал участие.
Вывожу на экран категории и среднее число инвестиционных раундов. Сортирую таблицу по возрастанию среднего. */

SELECT CASE
           WHEN invested_companies>=100 THEN 'high_activity'
           WHEN invested_companies>=20 THEN 'middle_activity'
           ELSE 'low_activity'
       END AS activity,
       ROUND(AVG(investment_rounds)) AS round_mean
FROM fund
GROUP BY activity
ORDER BY round_mean;


/* 10. Провожу анализ, в каких странах находятся фонды, которые чаще всего инвестируют в стартапы. 
Для каждой страны считаю минимальное, максимальное и среднее число компаний, в которые инвестировали фонды этой страны, основанные с 2010 по 2012 год включительно.
Исключаю страны с фондами, у которых минимальное число компаний, получивших инвестиции, равно нулю. 
Выгружаю десять самых активных стран-инвесторов: сортирую таблицу по среднему количеству компаний от большего к меньшему. Затем добавляю сортировку по коду страны в лексикографическом порядке. */

SELECT country_code,
       MIN(invested_companies),
       MAX(invested_companies),
       AVG(invested_companies)
FROM fund
WHERE CAST(DATE_TRUNC('year', founded_at) AS date) BETWEEN '2010-01-01' AND '2012-12-31' 
GROUP BY country_code
HAVING MIN(invested_companies) != 0
ORDER BY AVG(invested_companies) DESC, country_code
LIMIT 10;


/* 11. Отображаю имя и фамилию всех сотрудников стартапов.
Добавляю поле с названием учебного заведения, которое окончил сотрудник, если эта информация известна. */ 

SELECT p.first_name, p.last_name, e.instituition
FROM people AS p
LEFT JOIN education AS e ON p.id=e.person_id;

/* 12. Для каждой компании нахожу количество учебных заведений, которые окончили её сотрудники.
Вывожу название компании и число уникальных названий учебных заведений. Составляю топ-5 компаний по количеству университетов. */

WITH
pepl AS
     (SELECT p.company_id AS company, e.instituition AS instituition
     FROM people AS p
     LEFT JOIN education AS e ON p.id=e.person_id)

SELECT c.name, COUNT(DISTINCT p.instituition)
FROM company AS c
LEFT JOIN pepl AS p ON c.id=p.company
GROUP BY c.name
ORDER BY COUNT(DISTINCT p.instituition) DESC
LIMIT 5;

/* 13. Составляю список с уникальными названиями закрытых компаний, для которых первый раунд финансирования оказался последним. */

select DISTINCT c.name
FROM company AS c
LEFT JOIN funding_round AS f ON c.id=f.company_id
WHERE c.status = 'closed'
  AND f.is_last_round = 1
  AND f.is_first_round = 1;

/* 14. Составляю список уникальных номеров сотрудников, которые работают в компаниях, отобранных в предыдущем задании. */

WITH
comp AS
     (select DISTINCT c.id
      FROM company AS c
      LEFT JOIN funding_round AS f ON c.id=f.company_id
      WHERE c.status = 'closed'
        AND f.is_last_round = 1
        AND f.is_first_round = 1)

SELECT p.id
FROM people AS p
JOIN comp AS comp ON p.company_id=comp.id;

/* 15. Составляю таблицу, куда войдут уникальные пары с номерами сотрудников из предыдущей задачи и учебным заведением, которое окончил сотрудник. */

SELECT DISTINCT p.id, e.instituition
FROM people AS p
JOIN company AS c ON p.company_id=c.id
JOIN funding_round AS f ON c.id=f.company_id
RIGHT JOIN education AS e ON p.id=e.person_id
WHERE c.status = 'closed'
  AND f.is_last_round = 1
  AND f.is_first_round = 1;

/* 16. Считаю количество учебных заведений для каждого сотрудника из предыдущего задания.
При подсчёте учитываю, что некоторые сотрудники могли окончить одно и то же заведение дважды. */

SELECT DISTINCT p.id, COUNT(e.instituition)
FROM company AS c
JOIN people AS p ON c.id=p.company_id
LEFT JOIN education AS e ON p.id=e.person_id
WHERE c.status  = 'closed'
  AND c.id IN (SELECT company_id
               FROM funding_round
               WHERE is_first_round = 1
               AND is_last_round = 1)
  AND e.instituition IS NOT NULL
GROUP BY p.id;

/* 17. Дополняю предыдущий запрос и вывожу среднее число учебных заведений (всех, не только уникальных), которые окончили сотрудники разных компаний.
Вывожу только одну запись, группировка здесь не понадобится. */

SELECT AVG(count_int)
FROM (SELECT DISTINCT p.id,
      COUNT(e.instituition) AS count_int
      FROM company AS c
      JOIN people AS p ON c.id=p.company_id
      LEFT JOIN education AS e ON p.id=e.person_id
      WHERE c.status  = 'closed'
        AND c.id IN (SELECT company_id
       	             FROM funding_round
                     WHERE is_first_round = 1
                       AND is_last_round = 1)
        AND e.instituition IS NOT NULL
      GROUP BY p.id) AS fun;

/* 18. Пишу похожий запрос: вывожу среднее число учебных заведений (всех, не только уникальных), которые окончили сотрудники Facebook. */

WITH
func AS (SELECT person_id, COUNT(instituition) AS count_inst
         FROM education
         WHERE person_id IN (SELECT id
                             FROM people
                             WHERE company_id IN (SELECT id
                                                  FROM company
                                                  WHERE name = 'Facebook'))
         GROUP BY person_id)

SELECT AVG(count_inst)
FROM func;

/* 19. Составляю таблицу из полей:
name_of_fund — название фонда;
name_of_company — название компании;
amount — сумма инвестиций, которую привлекла компания в раунде.
В таблицу входят данные о компаниях, в истории которых было больше шести важных этапов, а раунды финансирования проходили с 2012 по 2013 год включительно. */

SELECT f.name AS name_of_fund,
       c.name AS name_of_company,
       fr.raised_amount AS amount
FROM investment AS i
JOIN company AS c ON i.company_id=c.id
JOIN fund AS f ON i.fund_id=f.id
JOIN funding_round AS fr ON i.funding_round_id=fr.id
WHERE c.milestones > 6
  AND EXTRACT(YEAR FROM fr.funded_at) BETWEEN 2012 AND 2013;

/* 20. Выгружаю таблицу, в которой будут такие поля:
название компании-покупателя;
сумма сделки;
название компании, которую купили;
сумма инвестиций, вложенных в купленную компанию;
доля, которая отображает, во сколько раз сумма покупки превысила сумму вложенных в компанию инвестиций, округлённая до ближайшего целого числа.
Не учитываю те сделки, в которых сумма покупки равна нулю. Если сумма инвестиций в компанию равна нулю, исключаю такую компанию из таблицы. 
Сортирую таблицу по сумме сделки от большей к меньшей, а затем по названию купленной компании в лексикографическом порядке. Ограничу таблицу первыми десятью записями. */

SELECT c1.name AS acquiring_company,
       a.price_amount,
       c2.name AS acquired_company,
       c2.funding_total,
       ROUND(a.price_amount / c2.funding_total) AS delta
FROM acquisition AS a
LEFT JOIN company AS c1 ON a.acquiring_company_id=c1.id
LEFT JOIN company AS c2 ON a.acquired_company_id=c2.id
where a.price_amount != 0  AND c2.funding_total != 0 AND c2.status = 'acquired'
ORDER BY a.price_amount DESC, acquired_company
LIMIT 10;

/* 21. Выгружаю таблицу, в которую войдут названия компаний из категории social, получившие финансирование с 2010 по 2013 год включительно.
Проверяю, что сумма инвестиций не равна нулю. Вывожу также номер месяца, в котором проходил раунд финансирования. */

SELECT c.name,
       EXTRACT(MONTH FROM fr.funded_at) AS month
FROM funding_round AS fr
JOIN company AS c ON fr.company_id=c.id
WHERE c.category_code = 'social'
  AND fr.raised_amount !=0
  AND EXTRACT(YEAR FROM fr.funded_at) BETWEEN 2010 AND 2013;

/* 22. Отбираю данные по месяцам с 2010 по 2013 год, когда проходили инвестиционные раунды. Группирую данные по номеру месяца и получаю таблицу, в которой будут поля:
номер месяца, в котором проходили раунды;
количество уникальных названий фондов из США, которые инвестировали в этом месяце;
количество компаний, купленных за этот месяц;
общая сумма сделок по покупкам в этом месяце. */

WITH
a AS (SELECT EXTRACT(MONTH FROM acquired_at) AS month,
             COUNT(acquired_company_id) as acq,
             SUM(price_amount) as total
      FROM acquisition
      WHERE EXTRACT(YEAR FROM acquired_at) BETWEEN 2010 AND 2013
      GROUP BY month),
      
fr AS (SELECT EXTRACT(MONTH FROM funded_at) AS month, id
       FROM funding_round
       WHERE EXTRACT(YEAR FROM funded_at) BETWEEN 2010 AND 2013),
       
i AS (SELECT funding_round_id, fund_id
      FROM investment
      WHERE funding_round_id IN (SELECT id
                                 FROM funding_round
                                 WHERE EXTRACT(YEAR FROM funded_at) BETWEEN 2010 AND 2013)
        AND fund_id IN (SELECT DISTINCT(id)
                       	FROM fund
                       	WHERE country_code = 'USA')),
                       
vol AS (SELECT month, COUNT(distinct(fund_id)) as fname
        FROM fr
        FULL OUTER JOIN i ON fr.id=i.funding_round_id
        GROUP BY month)                       
      
SELECT vol.month, fname, acq, total
FROM vol
LEFT JOIN a ON vol.month=a.month;

/* 23. Составляю сводную таблицу и вывожу среднюю сумму инвестиций для стран, в которых есть стартапы, зарегистрированные в 2011, 2012 и 2013 годах.
Данные за каждый год должны быть в отдельном поле таблицы. Сортирую таблицу по среднему значению инвестиций за 2011 год от большего к меньшему. */

WITH
i_1 AS (SELECT country_code AS country,
               AVG(funding_total) AS investment
        FROM company
        WHERE EXTRACT(YEAR FROM founded_at) = 2011
        GROUP BY country),
        
i_2 AS (SELECT country_code AS country,
               AVG(funding_total) AS investment
        FROM company
        WHERE EXTRACT(YEAR FROM founded_at) = 2012
        GROUP BY country),
        
i_3 AS (SELECT country_code AS country,
               AVG(funding_total) AS investment
        FROM company
        WHERE EXTRACT(YEAR FROM founded_at) = 2013
        GROUP BY country)

SELECT i_1.country,
       i_1.investment AS year_2011,
       i_2.investment AS year_2012,
       i_3.investment AS year_2013
FROM i_1 
JOIN i_2 ON i_1.country = i_2.country
JOIN i_3 ON i_2.country = i_3.country
ORDER BY year_2011 DESC;