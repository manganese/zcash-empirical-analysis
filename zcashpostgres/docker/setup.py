'''
d8888888P  a88888b.  .d888888  .d88888b  dP     dP
     .d8' d8'   `88 d8'    88  88.    "' 88     88
   .d8'   88        88aaaaa88a `Y88888b. 88aaaaa88a
 .d8'     88        88     88        `8b 88     88
d8'       Y8.   .88 88     88  d8'   .8P 88     88
Y8888888P  Y88888P' 88     88   Y88888P  dP     dP

This script sets up the zcash database
- Drops all previous tables and creates new ones based on table_classes.py
- You must have the database and user already created
'''

import config as conf
import dbLib
from table_classes import base
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

if __name__ == '__main__':
    # connect to database
    print("Checking if %s database is present" % conf.POSTGRES_DB)
    engine = create_engine(("postgres://postgres@localhost/%s" % conf.POSTGRES_DB))
    if not database_exists(engine.url):
        print("Database doesnt exist, creating...")
        create_database(engine.url)
    print("Connecting to %s database" % conf.POSTGRES_DB)
    con, meta = dbLib.connect(db=conf.POSTGRES_DB)
    # create the tables using the classs
    print("Dropping all previous tables")
    base.metadata.drop_all(con.engine)
    print("Creating new tables")
    base.metadata.create_all(con.engine)
    print("Complete")