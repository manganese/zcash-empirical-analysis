'''
d8888888P  a88888b.  .d888888  .d88888b  dP     dP
     .d8' d8'   `88 d8'    88  88.    "' 88     88
   .d8'   88        88aaaaa88a `Y88888b. 88aaaaa88a
 .d8'     88        88     88        `8b 88     88
d8'       Y8.   .88 88     88  d8'   .8P 88     88
Y8888888P  Y88888P' 88     88   Y88888P  dP     dP

Library used to connect to postgres
'''

import sqlalchemy
from sqlalchemy.orm import sessionmaker
import config as conf
from table_classes import Blocks
from sqlalchemy import func, exc
import sys


def connect(user='postgres', db=conf.POSTGRES_DB, host='localhost', port=5432):
    '''
    Connects to postgresql server
    Returns a connection and a metadata object
    '''
    # We connect with the help of the PostgreSQL URL
    # postgresql://federer:grandestslam@localhost:5432/tennis
    url = 'postgresql://{}@{}:{}/{}'
    url = url.format(user, host, port, db)
    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, client_encoding='utf8', pool_size=20, max_overflow=10)
    # We then bind the connection to MetaData()
    meta = sqlalchemy.MetaData(bind=con, reflect=True)
    return con, meta

def checkPostGreSQL():
    '''
    Checks if the postgresql server is up and running
    If not it just exits
    returns nothing
    '''
    try:
        # check if there is a server connection
        con, meta = connect()
        # check if tables exist
        # check for table blocks and transactions
        if con.has_table("blocks") and con.has_table("transactions"):
            print("Table \'blocks\' and \'transactions\' are present")
        else:
            print("Table \'blocks\' or \'transactions\' is missing, please check database")
            print("Exiting")
            sys.exit(1)
    except exc.SQLAlchemyError as e:
        print("SQLAlchemy error: ", e)
        print("Exiting")
        sys.exit(1)

def getSession():
    con, meta = connect()
    Session = sessionmaker(con)
    session = Session()
    return session

def addObjects(objects):
    print("Committing " +  str(len(objects)) + " objects to db")
    con, meta = connect()
    Session = sessionmaker(con)
    session = Session()
    try:
        session.bulk_save_objects(objects)
        session.commit()
        print "Commit success"
    except sqlalchemy.exc.IntegrityError as e:
        print e
        print "Integrity error when committing objects"
        print "Exiting"
        session.rollback()
        session.close()
        sys.exit(1)
        # for obj in objects:
        #     session.rollback()
        #     session.merge(obj)
        #     session.commit()
    except exc.SQLAlchemyError:
        print "Sql alchemy error, rolling back back back"
        session.rollback()
        session.close()
        sys.exit(1)
    session.close()

def getLatestBlockheight():
    print("Finding the latest block")
    session = getSession()
    result = session.query(func.max(Blocks.height)).first()
    if result[0] == None:
        return 0
    return result[0]
