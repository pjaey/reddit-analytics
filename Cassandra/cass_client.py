"""
cass_client.py: Provides interface for Cassandra functions
"""
import logging

from cassandra.cluster import Cluster

__author__ = 'pranjal'

LOG = logging.getLogger(__name__)


class CassClient:
    def __init__(self, hosts):
        """
        :param hosts: Cassandra hosts
        :type hosts: list
        """
        self._cluster = Cluster(hosts)
        self._session = self._cluster.connect()

    def insert(self, table_name, values):
        columns = values.keys()
        columns_string = ','.join(columns)
        values_string = "','".join([values[column] for column in columns])
        self._session.execute("INSERT INTO %s ( %s ) VALUES (' %s ')" % (table_name, columns_string, values_string))

    def select(self, table_name, columns, where_clause, limit=None):
        query = "SELECT " + columns + " FROM " + table_name
        if where_clause is not None:
            query += " WHERE " + where_clause
        if limit is not None:
            query += " LIMIT %s" % limit
        return self._session.execute(query)

    def create_key_space(self, key_space, replication_factor):
        self._session.execute("CREATE KEYSPACE %s WITH REPLICATION = "
                              "{'class': 'SimpleStrategy', 'replication_factor': %s};" % (key_space, replication_factor)
                              )

    def create_table(self, table_name, columns, primary_key, clustering_order=None):
        statement = "CREATE TABLE %s (%s, PRIMARY KEY (%s))" % (table_name, columns, primary_key)
        if clustering_order:
            statement += "WITH CLUSTERING ORDER BY (%s)" % clustering_order
        self._session.execute(statement)

