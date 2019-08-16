import pypyodbc
import pandas as pd


class Connection:
    def __init__(self, driver, server, database):
        self.db = None
        self.credentials = {'driver': driver,
                            'server': server,
                            'database': database}
        self.connect()

    def connect(self):
        self._initiate_connection(**self.credentials)

    def _initiate_connection(self, driver, server, database):
        self.db = pypyodbc.connect('Driver={};'
                                   'Server={};'
                                   'Database={};'.format(driver, server, database))

    def close(self):
        self.db.close()

    def query(self, sql, *args):
        for attempt in range(10):
            try:
                with self.db.cursor() as cursor:
                    cursor.execute(sql, args)
                    query_results = [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]
                df = pd.DataFrame(query_results)
            except pypyodbc.ProgrammingError:
                self.connect()
            else:
                break
        return df
