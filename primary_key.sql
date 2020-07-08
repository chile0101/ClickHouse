In every part data stored sorted lexicographically by primary key. For example, if your primary key — (CounterID, Date), than rows would be located sorted by CounterID, and for rows with the same CounterID — sorted by Date.

Data structure of primary key looks like an array of marks — it’s values of primary key every index_granularity rows.

index_granularity — settings of MergeTree engine, default to 8192.