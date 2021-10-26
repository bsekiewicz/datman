import json
import pandas as pd
from psycopg2 import connect, sql
from psycopg2.extras import execute_batch


class SQLDatabase(object):
    """
    Database PostgreSQL Utilities
    """

    connection_params_dict = None
    connection = None
    cursor = None

    def connect(self, connection_params):
        """
        Create connection to SQL database
        """
        if isinstance(connection_params, str):
            connection_params_dict = json.loads(connection_params)
        elif isinstance(connection_params, dict):
            connection_params_dict = connection_params
        else:
            raise TypeError(f"invalid connection_params type: {type(connection_params)}")

        if self.connection is not None:
            self.connection.close()

        self.connection_params_dict = connection_params_dict

        try:
            self.connection = connect(**connection_params_dict)
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
        except Exception as error:
            raise error
        else:
            print('Connection to sql database established')

    def reconnect(self):
        """
        Reconnect
        """
        if self.connection_params_dict is not None:
            if self.connection is not None:
                self.connection.close()

            try:
                self.connection = connect(**self.connection_params_dict)
                self.connection.autocommit = True
                self.cursor = self.connection.cursor()
            except Exception as error:
                raise error
            else:
                print('(Re)Connection to sql database established')

    def close(self):
        """
        Close connection
        """
        if self.connection is not None:
            self.connection.close()
        self.connection = None
        self.cursor = None
        print('Connection to sql database closed')
        
    def execute(self, sql_query: str):
        """Execute SQL task on connected database
        """
        self.cursor.execute(sql_query)
        return self.cursor.fetchall()

    def insert_in_batch(self, data: dict or list or pd.DataFrame, table_name: str, page_size=1000, verbose=0):
        """
        Sequential inserting to defined table
        """
        if self.connection is not None:
            if self.connection.closed == 1:
                self.reconnect()

        if isinstance(data, dict):
            if len(data) == 0:
                return True
            fields = [x for x in data.keys()]
            values = [data]
        elif isinstance(data, list):
            if len(data) == 0:
                return True
            fields = [x for x in data[0].keys()]
            values = data
        elif isinstance(data, pd.DataFrame):
            if data.shape[0] == 0:
                return True
            fields = list(data.columns)
            values = data.where(pd.notnull(data), None).to_dict(orient='records')
        else:
            print('Wrong data type!')
            return False

        sql_string = sql.SQL("INSERT INTO " + table_name + " ({}) VALUES ({})").format(
            sql.SQL(",").join(map(sql.Identifier, fields)),
            sql.SQL(",").join(map(sql.Placeholder, fields)))

        try:
            execute_batch(self.cursor, sql=sql_string, argslist=values, page_size=page_size)
            return True
        except Exception as e:
            print(e)
            if verbose == 1:
                print(data)
            return False

    def delete_in_batch(self, data: list, table_name: str):
        """Sequential removal from defined table"""
        if isinstance(data, list):
            fields = [x for x in data[0].keys()]
            values = data
        else:
            print('Wrong data type!')
            return False

        if len(fields) != 1:
            print('One filed is valid!')
            return False

        sql_string = sql.SQL("DELETE FROM " + table_name + " WHERE {}={}").format(
            sql.SQL(",").join(map(sql.Identifier, fields)),
            sql.SQL(",").join(map(sql.Placeholder, fields)))

        try:
            execute_batch(self.cursor, sql_string, values)
            return True
        except Exception as e:
            print(e)
            return False

    def duplicate_rows_select(self, table_name: str, partition: str):
        sql_query = """
            SELECT * FROM
              (SELECT *, count(*)
              OVER
                (PARTITION BY
                  {}
                ) AS count
              FROM {}) tableWithCount
              WHERE tableWithCount.count > 1;
        """.format(partition, table_name)
        self.cursor.execute(sql_query)
        return pd.DataFrame(self.cursor.fetchall())

    def duplicate_rows_delete(self, table_name: str, partition: str):
        sql_query = """
            DELETE 
            FROM {}
            WHERE ctid IN 
            (
                SELECT ctid 
                FROM(
                    SELECT 
                        *, 
                        ctid,
                        row_number() OVER (PARTITION BY {} ORDER BY ctid DESC) 
                    FROM {}
                )s
                WHERE row_number >= 2
            )        
        """.format(table_name, partition, table_name)
        return self.cursor.execute(sql_query)
