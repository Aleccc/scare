from datawarehouse.common import Connection

DRIVER = '{SQL Server}'
SERVER = 'GSDWPROD.gassouth.com'
DATABASE = 'GSDWSTG'

db = Connection(DRIVER, SERVER, DATABASE)
