from logging import Logger
import os

from src.database import db_sql
from src.env import AzurePartnerCenterEnv
from src.logger.logger import LOGGER

if os.environ['DATABASE_TYPE'] == 'sqlite3':
    import sqlite3 as sqllib
elif os.environ['DATABASE_TYPE'] == 'mssql':
    import pyodbc as sqllib
else:
    LOGGER.error(f'DATABASE_TYPE 환경변수 확인. 입력값: {os.environ["DATABASE_TYPE"]}')


class DBConnect:

    __db_instance = None

    # singleton pattern
    @classmethod
    def get_instance(cls):
        if cls.__db_instance is None:
            cls.__db_instance = cls()
        return cls.__db_instance

    def __init__(self):

        self._db_options = self._options()
        self._client = None
        self._connect_check()

    @property
    def get_client(self):
        self._connect()
        return self._client

    @staticmethod
    def get_sql():
        return db_sql

    def _connect(self):
        LOGGER.debug(f'Connect String {self._db_options}')
        LOGGER.info("Connecting to database...")
        if os.environ['DATABASE_TYPE'] == 'mssql':
            connection = sqllib.connect(self._db_options, encoding='utf8')
        else:
            connection = sqllib.connect(self._db_options)
        LOGGER.info('DB connect.')
        self._client = connection

    def close(self):
        if self._client:
            self._client.close()

    def commit(self):
        return self._client.commit()

    @staticmethod
    def _options():
        env = AzurePartnerCenterEnv.instance()
        if os.environ['DATABASE_TYPE'] == 'sqlite3':
            return f"{os.environ['DATABASE_NAME']}.db"
        elif os.environ['DATABASE_TYPE'] == 'mssql':
            # 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
            # return "DRIVER={SQL Server};"\
            return "DRIVER={ODBC Driver 17 for SQL Server};"\
                + f"SERVER={os.environ['DATABASE_HOST']},{os.environ['DATABASE_PORT']};"\
                + f"DATABASE={os.environ['DATABASE_NAME']};"\
                + f"UID={os.environ['DATABASE_USER']};"\
                + f"PWD={os.environ['DATABASE_PASSWORD']};"
            
            
            # {'host': os.environ['DATABASE_HOST'],
            #         'port': os.environ['DATABASE_PORT'],
            #         'user': os.environ['DATABASE_USER'],
            #         # 'password': os.environ['DATABASE_PASSWORD'],
            #         'password': env.database_password,
            #         'charset': 'utf8',
            #         'database': os.environ['DATABASE_NAME'],
            #         'autocommit': False
            #         }

    def _connect_check(func):
        def decorated(self, *args, **kwargs):
            try:
                self._client.cursor()
            except (sqllib.ProgrammingError, AttributeError):
                self._connect()
            return func(self, *args, **kwargs)
        return decorated

    @_connect_check
    def insert_data(self, sql, data, auto_commit=False):
        '''
        :param sql : str
        :param data: [(col1, col2, col2, ...), ...]  List(tuple)
        :param auto_commit:
        :return: bool is_success
        '''
        LOGGER.debug(f'Insert sql : {sql}')
        LOGGER.debug(f'data : {data}')
        for i in range(5):
            affected = -1

            try:
                cursor = self._client.cursor()
                if os.environ['DATABASE_TYPE'] == 'mssql':
                    cursor.fast_executemany = True
            except AttributeError:
                LOGGER.error('---------------SERVER CLOSED--------------------')
                raise
            except Exception as e:
                LOGGER.error(f"Error while obtaining cursor from database connection... {e}")
                raise

            try:
                LOGGER.info("Executing SQL INSERT...")
                affected = cursor.executemany(sql, data)
                if auto_commit:
                    self._client.commit()
                # cursor.close()
                LOGGER.info('Executing SQL INSERT Success')
                return affected
                # return affected
            # TODO: 나올수 있는 에러 정리 / 처리 ( Exception )
            except sqllib.Error as e:
                LOGGER.warning('[RETRY]INSERT SQL Error.. : %s' % e)
                # cursor.close()
                self._connect()
            except Exception as e:
                LOGGER.warning('[RETRY]DB Error.. : %s' % e)
                # cursor.close()
        LOGGER.error("Database Error...")
        LOGGER.exception('exception')
        raise sqllib.DatabaseError

    @_connect_check
    def delete_data(self, sql, data=None, auto_commit=False):
        '''
        :param sql : str
        :param data: tuple (col1, col2, col2, ...)
        :param auto_commit:
        :return: int affected
        '''
        LOGGER.debug(f'Delete sql : {sql}')
        LOGGER.debug(f'data : {data}')
        for i in range(5):
            try:
                cursor = self._client.cursor()
                if os.environ['DATABASE_TYPE'] == 'mssql':
                    cursor.fast_executemany = False
            except AttributeError:
                LOGGER.error('---------------SERVER CLOSED--------------------')
                raise
            except Exception as e:
                LOGGER.error(f"Error while obtaining cursor from database connection... {e}")
                raise

            try:
                LOGGER.info("Executing SQL DELETE...")
                if data:
                    affected = cursor.execute(sql, data)
                else:
                    affected = cursor.execute(sql)
                if auto_commit:
                    self._client.commit()
                # cursor.close()
                LOGGER.info('Executing SQL DELETE Success')
                return affected
            # TODO: 나올수 있는 에러 정리 / 처리 ( Exception )
            except sqllib.Error as e:
                LOGGER.warning('[RETRY]DELETE SQL Error.. : %s' % e)
                # cursor.close()
                self._connect()
            except Exception as e:
                LOGGER.warning('[RETRY]DB Error.. : %s' % e)
                # cursor.close()
        LOGGER.error("Database Error...")
        raise sqllib.DatabaseError

    @_connect_check
    def select_data(self, sql, data: tuple = None) -> dict:
        '''
        :param sql : str
        :param data: (col1, col2, col2, ...), ...  tuple
        :param auto_commit:
        :return: dict
        '''
        LOGGER.debug(f'Select sql : {sql}')
        LOGGER.debug(f'data : {data}')
        for i in range(5):
            try:
                if os.environ['DATABASE_TYPE'] == 'mysql':
                    cursor = self._client.cursor(sqllib.cursors.DictCursor)
                elif os.environ['DATABASE_TYPE'] == 'mssql':
                    cursor = self._client.cursor(as_dict=True)
                    cursor.fast_executemany = False
            except AttributeError:
                LOGGER.error('---------------SERVER CLOSED--------------------')
                raise
            except Exception as e:
                LOGGER.error(f"Error while obtaining cursor from database connection... {e}")
                raise

            try:
                LOGGER.info("Executing SQL SELECT...")
                cursor.execute(sql, data)
                rows = cursor.fetchall()
                # cursor.close()
                LOGGER.info('Executing SQL SELECT Success')
                return rows
            # TODO: 나올수 있는 에러 정리 / 처리 ( Exception )
            except sqllib.Error as e:
                LOGGER.warning('[RETRY]SELECT SQL Error.. : %s' % e)
                # cursor.close()
                self._connect()
            except Exception as e:
                LOGGER.warning('[RETRY]DB Error.. : %s' % e)
                # cursor.close()
        LOGGER.error("Database Error...")
        raise sqllib.DatabaseError


