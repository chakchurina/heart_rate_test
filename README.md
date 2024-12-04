# Тестовое задание 

# Что я думаю 

## age

Заметила, что age строкового типа в таблице, а в функции он int. Но я бы сделала не age типа int, а birth_date, потому что возраст меняется динамически.

## gender: str 

Пол объявлен в сигнатуре функции, но в описании функции не упомянут. Напрашивается сделать этот параметр опциональным, либо убрать его, так как легко представить ситуации, когда агрегация понадобится вне зависимости от пола

На всякий случай прошла регистрацию в Welltory, увидела три варианта: male / female / other и поняла, что внутри функциями по-хорошему нужно будет покрыться проверками на значения этого enum-а. А еще пишете, что функция должна возвращать объект пользователя, а не его id, то есть, результат будет содержать пол. Учитывая все это, я бы не стала усложнять функционал и фильтрацию по полу оставила бы уже пользователям функции.

## query_top

Думаю, что функцию нужно 
- или назвать query_top_10, и тогда зашитое в код функции магическое число 10 будет вызывать меньше вопросов, 
- или сделать 10 параметром, тем более, что top_n может измениться в будущем.

## Размеры таблиц

По дефолту apple watch вроед снимает пульс раз в 3-7 минут. Это генерирует нам (по порядку величины) ~10к записей на пользователя в месяц => 1k пользователей создают 10M записей ежемесячно. Объем таблицы получается такой, что лучше партиционировать. Разбивать думаю, лучше по датам, иначе создадим себе проблемы с масштабированием.

## Cкорость работы системы

Если запросы выполняются десятки секунд, я бы посмотрела, какие еще запросы мы делаем на эти данные: если запросы сложные, то можно рассмотреть переход на чистый сиквел, тогда может появиться больше пространства для оптимизации.

# Чистый SQL 

Так как запросы выполняются долго (десятки секунд), я сначала напишу код на чистом сиквеле, чтобы посмотреть EXPLAIN и убедиться, что перформанс запросов нельзя улучшить в песочнице.

```
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    gender VARCHAR CHECK (gender IN ('male', 'female', 'other')),
    birth_date DATE
);

CREATE INDEX idx_users_birth_date ON users (birth_date);


CREATE TABLE heart_rates_partitioned (
    id SERIAL,
    user_id INTEGER REFERENCES users(id),
    timestamp TIMESTAMP NOT NULL,
    heart_rate FLOAT NOT NULL,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE TABLE heart_rates_2024 PARTITION OF heart_rates_partitioned
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE INDEX idx_heart_rates_timestamp ON heart_rates_partitioned 
(timestamp);


WITH heart_rate_avg AS (
    SELECT
        user_id,
        AVG(heart_rate) AS avg_heart_rate
    FROM
        heart_rates_partitioned
    WHERE
        timestamp BETWEEN '2024-01-01' AND '2024-12-01'
    GROUP BY
        user_id
    HAVING
        AVG(heart_rate) > 70
)
SELECT
    u.*
FROM
    users u
JOIN
    heart_rate_avg h ON u.id = h.user_id
WHERE
    EXTRACT(YEAR FROM AGE(u.birth_date)) > 30;


WITH hourly_avg_heart_rate AS (
    SELECT
        user_id,
        DATE_TRUNC('hour', timestamp) AS hour_slot,
        AVG(heart_rate) AS avg_heart_rate
    FROM
        heart_rates_partitioned
    WHERE
        timestamp BETWEEN '2024-01-01' AND '2024-12-31' 
        AND user_id = 123 
    GROUP BY
        user_id,
        hour_slot
)
SELECT
    hour_slot,
    avg_heart_rate
FROM
    hourly_avg_heart_rate
ORDER BY
    avg_heart_rate DESC
LIMIT 10;
```

# SQLAlchemy

Теперь напишу то же самое на алхимии, раз заявлена алхимия. Результат в файле
