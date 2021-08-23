# -*- coding: utf-8 -*-

import json
import pandas as pd
from psycopg2 import connect, sql
from psycopg2.extras import execute_batch


class SQLDatabase(object):
    """Database PostgreSQL Utilities
    """

    connection = None
    cursor = None

    def connect(self, connection_params):
        """Create connection to SQL database
        """
        if isinstance(connection_params, str):
            connection_params_dict = json.loads(connection_params)
        elif isinstance(connection_params, dict):
            connection_params_dict = connection_params
        else:
            raise TypeError(f"invalid connection_params type: {type(connection_params)}")

        if self.connection is not None:
            self.connection.close()

        self.connection = connect(**connection_params_dict)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

    def close(self):
        """Close connection
        """
        if self.connection is not None:
            self.connection.close()
        self.connection = None
        self.cursor = None

    def execute(self, sql_query: str):
        """Execute SQL task on connected database
        """
        self.cursor.execute(sql_query)
        return self.cursor.fetchall()

    def insert_batch(self, data: list, table_name: str):
        """Sequential inserting to defined table
        """
        if isinstance(data, dict):
            if len(data) == 0:
                return True
            fields = [x for x in data.keys()]
            values = [data]
        elif isinstance(data, list):
            if len(data) == 0:
                return True
            # TODO: consistency of keys 
            fields = [x for x in data[0].keys()]
            values = data
        elif isinstance(data, pd.DataFrame):
            if data.shape[0] == 0:
                return True
            fields = list(data.columns)
            values = data.fillna("NULL").to_dict(orient='index')
            values = [values[x] for x in values]
        else:
            print('Wrong data type!')
            return False

        sql_string = sql.SQL("INSERT INTO " + table_name + " ({}) VALUES ({})").format(
            sql.SQL(",").join(map(sql.Identifier, fields)),
            sql.SQL(",").join(map(sql.Placeholder, fields)))

        try:
            execute_batch(self.cursor, sql_string, values)
            return True
        except Exception as e:
            print(e)
            print(data)
            return False

    def delete_batch(self, data, table_name):
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
