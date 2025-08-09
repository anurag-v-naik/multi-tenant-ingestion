import psycopg2
import psycopg2.extras
from typing import Dict, Any, List, Optional
from .base import BaseConnector, ConnectorError


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL database connector with tenant isolation"""

    def __init__(self, config: Dict[str, Any], tenant_id: str):
        super().__init__(config, tenant_id)
        self.required_config = ['host', 'port', 'database', 'user', 'password']
        self._validate_config()

    def _validate_config(self):
        """Validate required configuration parameters"""
        missing = [key for key in self.required_config if key not in self.config]
        if missing:
            raise ConnectorError(f"Missing required configuration: {missing}")

    def connect(self) -> bool:
        """Establish connection to PostgreSQL database"""
        try:
            # Add tenant prefix to database name for isolation
            database_name = f"{self.tenant_id}_{self.config['database']}"

            connection_params = {
                'host': self.config['host'],
                'port': self.config['port'],
                'database': database_name,
                'user': self.config['user'],
                'password': self.config['password'],
                'connect_timeout': self.config.get('timeout', 30)
            }

            self.connection = psycopg2.connect(**connection_params)
            self.connection.set_session(autocommit=False)
            self.is_connected = True

            self.logger.info(f"Connected to PostgreSQL database: {database_name}")
            return True

        except psycopg2.Error as e:
            self.logger.error(f"PostgreSQL connection failed: {e}")
            self.is_connected = False
            return False

    def disconnect(self) -> None:
        """Close PostgreSQL connection"""
        if self.connection:
            try:
                self.connection.close()
                self.is_connected = False
                self.logger.info("Disconnected from PostgreSQL")
            except psycopg2.Error as e:
                self.logger.error(f"Error disconnecting from PostgreSQL: {e}")

    def test_connection(self) -> bool:
        """Test PostgreSQL connection"""
        if not self.is_connected:
            return self.connect()

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except psycopg2.Error as e:
            self.logger.error(f"Connection test failed: {e}")
            self.is_connected = False
            return False

    def extract_data(self, query: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Extract data using SQL query"""
        if not self.is_connected and not self.connect():
            raise ConnectorError("Cannot connect to database")

        try:
            with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Add limit to query if specified
                if limit:
                    query = f"{query.rstrip(';')} LIMIT {limit}"

                self.logger.debug(f"Executing query: {query}")
                cursor.execute(query)

                results = cursor.fetchall()
                return [dict(row) for row in results]

        except psycopg2.Error as e:
            self.logger.error(f"Query execution failed: {e}")
            raise ConnectorError(f"Query failed: {e}")

    def get_schema(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Get schema information for tables"""
        if not self.is_connected and not self.connect():
            raise ConnectorError("Cannot connect to database")

        try:
            with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                if table_name:
                    # Get schema for specific table
                    query = """
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                    """
                    cursor.execute(query, (table_name,))
                    columns = cursor.fetchall()

                    return {
                        "table": table_name,
                        "columns": [dict(col) for col in columns]
                    }
                else:
                    # Get schema for all tables
                    query = """
                    SELECT table_name, column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()

                    schema = {}
                    for row in results:
                        table = row['table_name']
                        if table not in schema:
                            schema[table] = []
                        schema[table].append({
                            'column_name': row['column_name'],
                            'data_type': row['data_type'],
                            'is_nullable': row['is_nullable']
                        })

                    return schema

        except psycopg2.Error as e:
            self.logger.error(f"Schema query failed: {e}")
            raise ConnectorError(f"Schema retrieval failed: {e}")

    def get_table_list(self) -> List[str]:
        """Get list of available tables"""
        if not self.is_connected and not self.connect():
            raise ConnectorError("Cannot connect to database")

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)

                tables = cursor.fetchall()
                return [table[0] for table in tables]

        except psycopg2.Error as e:
            self.logger.error(f"Table list query failed: {e}")
            raise ConnectorError(f"Failed to get table list: {e}")

    def validate_tenant_isolation(self) -> bool:
        """Validate tenant data isolation for PostgreSQL"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT current_database()")
                current_db = cursor.fetchone()[0]
                expected_db = f"{self.tenant_id}_{self.config['database']}"

                if current_db != expected_db:
                    self.logger.error(f"Tenant isolation violation: connected to {current_db}, expected {expected_db}")
                    return False

                self.logger.info(f"Tenant isolation validated for database: {current_db}")
                return True

        except psycopg2.Error as e:
            self.logger.error(f"Tenant isolation validation failed: {e}")
            return False
