from Cassandra.cass_client import CassClient
import config

"""
cass_setup.py: Create the necessary Cassandra keyspaces and tables if they do not exist
"""


class CassSetup:
    def __init__(self):
        self.cass_client = CassClient([self._read_from_config("cassandra_host", "localhost")])

    @staticmethod
    def _read_from_config(key, default_value):
        try:
            return config.settings[key]
        except KeyError:
            return default_value

    def execute(self):
        for key_space in config.cass_create["create_key_spaces"]:
            replication_factor = CassSetup._read_from_config("cass_replication_factor", '1')
            self.create_key_space_if_not_exists(key_space, replication_factor)

        for table, attributes in config.cass_create["create_tables"].iteritems():
            self.create_table_if_not_exists(table, attributes["columns"], attributes["primary_key"],
                                            attributes["clustering_order"])

    def create_key_space_if_not_exists(self, key_space, replication_factor):
        exists = len(self.cass_client.select("system.schema_keyspaces", "*", "keyspace_name = '" + key_space + "'")) > 0
        if not exists:
            self.cass_client.create_key_space(key_space, replication_factor)

    def create_table_if_not_exists(self, table_full_name, columns, primary_key, clustering_order):
        key_space, table_name = table_full_name.split(".")
        sys_table_name = "system.schema_columnfamilies"
        where_clause = "keyspace_name = '" + key_space + "' and columnfamily_name = '" + table_name + "'"
        exists = len(self.cass_client.select(sys_table_name, "*", where_clause)) > 0
        if not exists:
            self.cass_client.create_table(table_full_name, columns, primary_key, clustering_order)


if __name__ == "__main__":
    cassClient = CassSetup()
    cassClient.execute()

