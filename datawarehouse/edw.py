from datawarehouse.common import Connection

DRIVER = '{SQL Server}'
SERVER = 'GSDWPROD.gassouth.com'
DATABASE = 'EDW'

db = Connection(DRIVER, SERVER, DATABASE)
