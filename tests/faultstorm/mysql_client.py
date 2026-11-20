import pymysql

from faultstorm.db_client import DatabaseClient


class MySQLClient(DatabaseClient):
    nodes = ["mysql1", "mysql2", "mysql3"]

    def get_db_nodes(self):
        return self.nodes

    def connect(self, node):
        return pymysql.connect(
            host=node, user="admin", password="admin_pwd", database="test1",
            autocommit=True, connect_timeout=3, read_timeout=5, write_timeout=5,
        )

    def setup(self, node):
        with self.connect(node) as db, db.cursor() as cursor:
            cursor.execute("CREATE TABLE IF NOT EXISTS faultstorm_set (value BIGINT PRIMARY KEY)")
            cursor.execute("TRUNCATE TABLE faultstorm_set")

    def add(self, node, value):
        with self.connect(node) as db, db.cursor() as cursor:
            cursor.execute("INSERT INTO faultstorm_set VALUES (%s)", (value,))

    def read(self, node):
        with self.connect(node) as db, db.cursor() as cursor:
            cursor.execute("SELECT value FROM faultstorm_set")
            return {row[0] for row in cursor.fetchall()}

    def is_definite_failure(self, exc):
        return isinstance(exc, pymysql.err.OperationalError) and exc.args[0] in (1290, 1792)
