import psycopg2
import sys
import pandas as pd
class DATABASE_CONNECTOR:
    def __init__(self,hostname=None,port=5432,user="postgres",password=None,database="postgres") -> None:
        self.hostname = hostname
        self.port = port
        self.user = user
        self.password = password
        self.database = database
    
    def makeParameter(self):
        if (self.password != None):
            self.params_dic = {"host":self.hostname,"database": self.database,"user": self.user,"password": self.password,"port":self.port}
        else:
            self.params_dic = {"host":self.hostname,"database": self.database,"user": self.user,"port":self.port}

class POSTGRES_CONNECTOR(DATABASE_CONNECTOR):
    def __init__(self, hostname=None, port=5432, user="postgres", password=None, database="postgres") -> None:
        super().__init__(hostname=hostname, port=port, user=user, password=password, database=database)

    def makeConnect(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**self.params_dic)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            sys.exit(1) 
        print("Connection successful")
        return conn
    @staticmethod
    def postgresql_to_dataframe(conn, select_query, column_names):
        """
        Tranform a SELECT query into a pandas dataframe
        """
        cursor = conn.cursor()
        try:
            cursor.execute(select_query)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            cursor.close()
            return 1
        # Naturally we get a list of tupples
        tupples = cursor.fetchall()
        cursor.close()
        # We just need to turn it into a pandas dataframe
        df = pd.DataFrame(tupples, columns=column_names)
        return df
