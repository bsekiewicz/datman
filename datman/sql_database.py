import json
import logging
import pandas as pd
from psycopg2 import connect, sql
from psycopg2.extras import execute_batch, execute_values
from mlracing.constants import *


class SQLDatabase(object):
    """
    """
    connection = None
    cursor = None
    connection_params_dict = None
    dbname = ''

    def connect(self, connection_params, verbose=1):
        """
        """
        if isinstance(connection_params, str):
            connection_params_dict = json.loads(connection_params)
        elif isinstance(connection_params, dict):
            connection_params_dict = connection_params
        else:
            raise TypeError(f"Invalid connection_params type: {type(connection_params)}")

        # one connection per class
        if self.connection is not None:
            self.connection.close()

        self.connection_params_dict = connection_params_dict
        self.dbname = connection_params_dict.get('dbname', '')

        try:
            self.connection = connect(**connection_params_dict)
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
        except Exception as error:
            raise error
        else:
            if verbose >= 1:
                logging.info(f'Connection to {self.dbname} database established')

    def close(self, verbose=1):
        """
        """
        if self.connection is not None:
            self.connection.close()

        self.connection = None
        self.cursor = None
        if verbose >= 1:
            logging.info(f'Connection to {self.dbname} database closed')

    def insert_in_batch(self, data: dict or list or pd.DataFrame, table_name: str, page_size=10000, verbose=1):
        """
        """
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
            if verbose >= 1:
                logging.warning('Wrong data type!')
            return False

        sql_string = sql.SQL("INSERT INTO " + table_name + " ({}) VALUES ({})").format(
            sql.SQL(",").join(map(sql.Identifier, fields)),
            sql.SQL(",").join(map(sql.Placeholder, fields)))

        try:
            execute_batch(self.cursor, sql=sql_string, argslist=values, page_size=page_size)
            return True
        except Exception as e:
            if verbose >= 1:
                logging.warning(e)
            if verbose >= 2:
                logging.warning(data)
            return False
        
    def update_in_batch(self, data: list, columns_set: list, columns_where: list,
                        table_name: str, page_size=10000, verbose=1):
        """
        """
        if len(columns_set) == 0:
            if verbose >= 1:
                logging.warning('No columns_set')
            return False

        if len(columns_where) == 0:
            if verbose >= 1:
                logging.warning('No columns_where')
            return False

        if isinstance(data, list):
            values = [tuple(x[y] for y in data[0].keys()) for x in data]
            fields = list(data[0].keys())

            sql_fields = ', '.join(fields)
            sql_set = ', '.join([f'{x[0]} = data.{x[1]}' for x in columns_set])
            sql_where = ' AND '.join([f'{table_name}.{x} = data.{x}' for x in columns_where])
        else:
            if verbose >= 1:
                logging.warning('Wrong data type!')
            return False

        try:
            execute_values(
                self.cursor,
                f'''
                    UPDATE {table_name} 
                    SET
                        {sql_set} 
                    FROM (VALUES %s) AS data ({sql_fields}) 
                    WHERE
                        {sql_where} 
                ''',
                values,
                page_size=page_size
            )
            return True
        except Exception as e:
            if verbose >= 1:
                logging.warning(e)
            if verbose >= 2:
                logging.warning(data)
            return False
        
    def delete_in_batch(self, data, table_name, page_size=1000, verbose=1):
        """
        """
        if isinstance(data, list):
            fields = [x for x in data[0].keys()]
            values = data
        else:
            if verbose >= 1:
                logging.warning('Wrong data type!')
            return False

        if len(fields) != 1:
            if verbose >= 1:
                logging.warning('One filed is valid!')
            return False

        sql_string = sql.SQL("DELETE FROM " + table_name + " WHERE {}={}").format(
            sql.SQL(",").join(map(sql.Identifier, fields)),
            sql.SQL(",").join(map(sql.Placeholder, fields)))

        try:
            execute_batch(self.cursor, sql=sql_string, argslist=values, page_size=page_size)
            return True
        except Exception as e:
            if verbose >= 1:
                logging.warning(e)
            return False
