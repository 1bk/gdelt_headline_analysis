###################################################
###################################################
###                                             ###
###   Simple SQlite Database                    ###
###   Adapted for GDELT mentions.CSV            ###
###   By Lee Boon Keong                         ###
###   Based on: https://www.codementor.io/      ###
###                                             ###
###################################################
###################################################

import sqlite3
import csv
import sys

class DBHelper:
    def __init__(self, dbname="gdelt.sqlite"):
        self.dbname = dbname
        self.conn = sqlite3.connect(dbname)

    def setup(self):
        stmt = "CREATE TABLE IF NOT EXISTS db (GLOBALEVENTID text,\
                                                    EventTimeDate text,\
                                                    MentionTimeDate text,\
                                                    MentionType text,\
                                                    MentionSourceName text,\
                                                    MentionIdentifier text,\
                                                    SentenceID text,\
                                                    Actor1CharOffset text,\
                                                    Actor2CharOffset text,\
                                                    ActionCharOffset text,\
                                                    InRawText text,\
                                                    Confidence text,\
                                                    MentionDocLen text,\
                                                    MentionDocTone text,\
                                                    MentionDocTranslationInfo text,\
                                                    Extras text)"
        self.conn.execute(stmt)
        self.conn.commit()

    def add_csv(self, fname = None):
        if fname == None:
            sys.exit('Error! No file name entered. Exiting...')
        else:
            f = open(fname,'r')
        
        reader = csv.reader(f, delimiter='\t')

        stmt = "INSERT INTO db VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        for row in reader:
	        self.conn.execute(stmt, row) 
        
        f.close()
        self.conn.commit()
        
    def get_row(self, num_row = None):
        if num_row == None:
            num_row = input('Enter the number of rows to print: \n')
        
        stmt = "SELECT * FROM db LIMIT " + str(num_row)
        # return [x[0] for x in self.conn.execute(stmt)]
        for row in self.conn.execute(stmt):
            print(row)
        # self.conn.commit()
        
    def get_specific(self, site1, site2):
        stmt = "SELECT MentionSourceName, MentionIdentifier FROM db \
                WHERE MentionSourceName = \'" + str(site1) + \
                "\' OR MentionSourceName = \'" + str(site2) + "\'"
        # return [x[0] for x in self.conn.execute(stmt)]
        return self.conn.execute(stmt)
            
    def count_row(self):
        stmt = "SELECT count(*) as row_count FROM db"
        return [x[0] for x in self.conn.execute(stmt)]
        # self.conn.commit()

    def remove_duplicates(self):
        # Here 'rowid' is used instead of 'ctid' - which we use for Postgres
        stmt = 'DELETE FROM db WHERE rowid NOT IN (SELECT min(rowid) FROM db GROUP BY MentionIdentifier)' 
        self.conn.execute(stmt)
        self.conn.commit()

    def add_col(self, colname = 'New_Column'):
        try: 
            stmt = "ALTER TABLE db ADD COLUMN " + colname + " text"
            self.conn.execute(stmt)
            self.conn.commit()
        except:
            pass
            print('Column {} likely already exists. No new column created.\n'.format(colname))

    def del_col(self, colname):
        try: 
            stmt = "ALTER TABLE db DELETE COLUMN " + colname + " text"
            self.conn.execute(stmt)
            self.conn.commit()
        except:
            pass
            print('Column {} likely already exists. No new column created.\n'.format(colname))

    def list_col(self):
        stmt = "PRAGMA table_info(db)"
        # return [x[0] for x in self.conn.execute(stmt)]
        return self.conn.execute(stmt) # Returns a list, remember to use "list()" in python script.

    def list_source(self, order = "DECS"):
        stmt = "SELECT MentionSourceName, count(*) FROM db GROUP BY MentionSourceName ORDER BY 2 "+ order
        return self.conn.execute(stmt)

    def create_df(self):
        stmt = "SELECT MentionSourceName, MentionIdentifier FROM db"
        return self.conn.execute(stmt)

