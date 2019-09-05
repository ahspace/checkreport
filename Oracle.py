import cx_Oracle
import logging
import logging_ini

class Oracle(object):
    db = None
    def connect(self, username, password, hostname, port, servicename):
        """ Connect to the database. """
        try:
            dsnStr=cx_Oracle.makedsn(hostname,port,servicename)
            self.db = cx_Oracle.connect(username, password, dsn=dsnStr)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 1017:
                pass
            else:
                pass
            # Very important part!
            raise

        # If the database connection succeeded create the cursor
        # we-re going to use.
        self.cursor = self.db.cursor()
        #self.cursor.execute("ALTER SESSION SET NLS_LANGUAGE='FRENCH_FRANCE.UTF8'")

    def disconnect(self):
        """
        Disconnect from the database. If this fails, for instance
        if the connection instance doesn't exist we don't really care.
        """
        try:
            if self.db is not None:
                self.cursor.close()
                self.db.close()
        except cx_Oracle.DatabaseError:
            pass

    def execute(self, sql, bindvars=None,realargs=None, commit=False):
        """
        Execute whatever SQL statements are passed to the method;
        commit if specified. Do not specify fetchall() in here as
        the SQL statement may not be a select.
        bindvars is a dictionary of variables you pass to execute.
        """
        try:
            if realargs is not None:
                realvars = {}
                for arg in realargs:
                    realvars[arg] = bindvars[arg]
                return self.cursor.execute(sql,**realvars)
            elif bindvars is not None:
                return self.cursor.execute(sql,**bindvars)
            else:
                return self.cursor.execute(sql)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print('Database connection error: %s' % format(e))
            print(e) 
            print(error.code)
            print(error.message)
            print(error.context)
            print(error.offset)
            if error.code == 955:
                pass
            elif error.code == 1031:
                pass

            # Raise the exception.
            raise
