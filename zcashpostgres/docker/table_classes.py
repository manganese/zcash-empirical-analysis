'''
d8888888P  a88888b.  .d888888  .d88888b  dP     dP
     .d8' d8'   `88 d8'    88  88.    "' 88     88
   .d8'   88        88aaaaa88a `Y88888b. 88aaaaa88a
 .d8'     88        88     88        `8b 88     88
d8'       Y8.   .88 88     88  d8'   .8P 88     88
Y8888888P  Y88888P' 88     88   Y88888P  dP     dP

PostGreSQL tables represented as classes
'''

from sqlalchemy import Column, String, Integer, BigInteger, ARRAY, JSON, Float, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
base = declarative_base()

class Blocks(base):
    __tablename__ = 'blocks'

    height = Column('height', BigInteger, primary_key=True)
    hash = Column('hash', String)
    size = Column('size', Integer)
    version = Column('version', Integer)
    merkleroot = Column('merkleroot', String)
    tx = Column('tx', ARRAY(String))
    time = Column('time', BigInteger)
    nonce = Column('nonce', String)
    #solution = Column('solution', String)
    #bits = Column('bits', String)
    difficulty = Column('difficulty', BigInteger)
    chainwork = Column('chainwork', String)
    anchor = Column('anchor', String)
    previousblockhash = Column('previousblockhash', String)
    nextblockhash = Column('nextblockhash', String)


class Transactions(base):
    __tablename__ = 'transactions'

    tx_hash = Column('tx_hash', String, primary_key=True)
    version = Column('version', Integer)
    locktime = Column('locktime', Integer)
    blockhash = Column('blockhash', String)
    time = Column('time', Integer)
    blocktime = Column('blocktime', Integer)
    #vin = Column('vin', JSON)
    #vjoinsplit = Column('vjoinsplit', JSON)
    #vout = Column('vout', JSON)
    valid = Column('valid', Boolean)
    tx_vin_count = Column('tx_vin_count', Integer)
    tx_vj_count = Column('tx_vj_count', Integer)
    tx_vout_count = Column('tx_vout_count', Integer)

    # fee
    # = Total VIN-VOUT
    #fee = Column('fee', Float)
    # Number of VIN
    #noInputs = Column('noInputs', Integer)
    # Number of VOUT
    #noOutputs = Column('noOutputs', Integer)
    # type # type of transaction
    #   MinerReward -> newly generated coins, has coinbase as input
    #   ValueTransfer -> Vin is a transaction
    type = Column('type', String)
    # total transparent output - output in the blockchain
    #outputValue = Column('outputValue', Float)
    # boolean, if contains shielded then true, if vjoinsplit then true
    #shielded = Column('shielded', Boolean)
    # value given into shielded
    #shieldedValue = Column('shieldedValue', Float)
    # computed from total VIN
    #value = Column('value', Float)

class Coingen(base):
    __tablename__ = "coingen"

    # db id
    db_id = Column('db_id', Integer, autoincrement=True)
    tx_hash = Column('tx_hash', String, ForeignKey(Transactions.tx_hash), primary_key=True)
    coinbase = Column('coinbase', String)
    sequence = Column('sequence', BigInteger)
    transaction = relationship('Transactions', foreign_keys='Coingen.tx_hash')


class Vjoinsplit(base):
    __tablename__ = "vjoinsplit"

    # db id
    db_id = Column('db_id', Integer, autoincrement=True, primary_key=True)
    tx_hash = Column('tx_hash', String, ForeignKey(Transactions.tx_hash))
    vpub_old = Column('vpub_old', Float)
    vpub_new = Column('vpub_new', Float)
    anchor = Column("anchor", String)
    nullifiers  = Column('nullifiers', ARRAY(String))
    commitments = Column('commitments', ARRAY(String))
    onetimePubKey = Column("onetimePubKey", String)
    randomSeed =  Column("randomSeed", String)
    macs = Column('macs', ARRAY(String))
    transaction = relationship('Transactions', foreign_keys='Vjoinsplit.tx_hash', backref="vjoinsplit")
    src_address = Column('source_address', ARRAY(String))
    dest_address = Column('dest_address', ARRAY(String))
    # proof = Column("proof", String)
    # ciphertexts = Column('ciphertexts', ARRAY(String))

class Vin(base):
    __tablename__ = "vin"

    # db id
    db_id = Column('db_id', Integer, autoincrement=True)
    # this is the tx where the vin goes into
    tx_hash = Column('tx_hash', String, ForeignKey(Transactions.tx_hash), primary_key=True)
    # this is the source of the vin, the vout it came from
    prevout_hash = Column('prevout_hash', String, primary_key=True)
    # this is vin["vout"]
    prevout_n = Column('prevout_n', Integer, primary_key=True)
    sequence = Column('sequence', BigInteger)
    # script = Column('script', String)
    address = Column('address', ARRAY(String))

    transaction = relationship('Transactions', foreign_keys='Vin.tx_hash', backref="vin")

class Vout(base):
    __tablename__ = "vout"
    # db id
    db_id = Column('db_id', Integer, autoincrement=True)
    # this is the tx where the vout came from
    tx_hash = Column('tx_hash', String, ForeignKey(Transactions.tx_hash), primary_key=True)
    value = Column('value', Float)
    valueZat = Column('valueZat', Float)
    # vout["n"]
    tx_n = Column('tx_n', Integer, primary_key=True)
    # scriptPubKey addresses
    pubkey = Column('pubkey', ARRAY(String))
    # scriptPubKey [hex] decoded
    # #script = Column('script', String)
    isVjoinsplit = Column('isVjoinsplit', Boolean)
    transaction = relationship('Transactions', foreign_keys='Vout.tx_hash', backref="vout")

TX_TYPE_MINERREWARD = "minerReward"
TX_TYPE_PUBLIC = "public"
TX_TYPE_SHIELDED = "shielded"
TX_TYPE_DESHIELDED = "deshielded"
TX_TYPE_PRIVATE = "private"
TX_TYPE_MIXED = "mixed"