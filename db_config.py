#CONFIGURAÇÃO DO BANCO DE DADOS
import pyodbc

def connect_to_db():
    conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};'
                          'SERVER=dbs-sddo-dev.database.windows.net,1433;'
                          'DATABASE=db-sddo-dev;'
                          'UID=sennatechadmin;'
                          'PWD=adm@Sennatech;')
    return conn

def disconnect_from_db(conn):
    conn.close()
