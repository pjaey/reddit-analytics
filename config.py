"""
config.py: contains settings and Cassandra keyspace and table creation data
"""
KEY_SPACE = "reddit_analytics"

settings = {
    # defaults to localhost
    "cassandra_host": "localhost",
    # defaults to "1"
    "cass_replication_factor": "1"
}

cass_create = {
    "create_key_spaces": [KEY_SPACE],
    "create_tables": {
        KEY_SPACE + ".comments":
            {
                # format: <column_name> <column_type>, <column_name> <column_type>, ...
                "columns": "",
                # format: <column_name>, <column_name>, ...
                "primary_key": "",
                # format: <column_name> <ASC/DESC>, <column_name> <ASC/DESC>, ...
                "clustering_order": ""
            },
        KEY_SPACE + ".submissions":
            {
                "columns": "",
                "primary_key": "",
                "clustering_order": ""
            }
    }
}