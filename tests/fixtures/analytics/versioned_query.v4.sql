SELECT
    date,
    user_id,
    CAST(events AS FLOAT64) as events,
    session_count
FROM source_table
WHERE date = @partition_date
