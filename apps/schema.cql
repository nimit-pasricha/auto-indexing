CREATE KEYSPACE IF NOT EXISTS index_optimizer 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE index_optimizer;

CREATE TABLE IF NOT EXISTS query_stats (
    table_name text,
    window_start timestamp,
    window_end timestamp,
    column_name text,
    operator text,
    query_count int,
    PRIMARY KEY ((table_name), window_start, window_end, column_name, operator)
) WITH default_time_to_live = 86400;