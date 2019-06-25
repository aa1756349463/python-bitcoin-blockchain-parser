import os
from blockchain_parser.blockchain import Blockchain
import datetime
import leveldb
from binascii import a2b_hex, b2a_hex
import pymongo
from pymongo import MongoClient
import subprocess
import urllib

start = datetime.datetime.now()
blockchain = Blockchain('/data1/bitcoin/blocks/')
index_path = '/data1/bitcoin/blocks/index'
back_path = '/data/workdir/data/bitcoinparser/bitcoin_index_backup/index'
txhash_address = leveldb.LevelDB("/data/workdir/data/bitcoinparser/leveldb/")

block_column = ["height","hash","version","previous_block_hash","merkle_root","timestamp","bits","difficulty","nonce","n_transactions"]
tx_header_column = ["height","tx_index","version","locktime","txhash","n_inputs","n_outputs","is_segwit"]
tx_output_column = ["height","tx_index","txhash","output_num","value","address","script","type"]
tx_input_column = ["height","tx_index","input_num","txhash","transaction_index","sequence_number","script","witnesses","value","address"]


parser_result_path = '/data/workdir/data/bitcoinparser/parse_result_auto2/'

DB_NAME = "bitcoin"
BLOCK = "block"
TX_HEADER = "tx_header"
TX_INPUT = "tx_input"
TX_OUTPUT = "tx_output"

def initMongo(client, collection):
    db = client[DB_NAME]
    try:
        db.create_collection(collection)
    except:
        pass
    return db[collection]

def insertMongo(client, d, column_name):
    document = {column:d[idx] for idx, column in enumerate(column_name)}
    try:
        client.insert_one(document)
        return None
    except Exception as err:
        print(err)

def insertMongoMany(client, documents, column_name):
    document = [{column:d[idx] for idx, column in enumerate(column_name)} for d in documents]
    try:
        client.insert_many(document)
        return None
    except Exception as err:
        print(err)

def update_input_balance(client, address, value, height):
    client.update({'address':address},{'$inc':{'value_satoshi':-value}, '$set':{'last_height':height}}, upsert=True) 

def update_output_balance(client, address, value, height):
    client.update({'address':address},{'$inc':{'value_satoshi':value}, '$set':{'last_height':height}}, upsert=True)

# delete old index file
try:
    os.system('rm -rf {}'.format(back_path))
except:
    pass

# load new index files
try:
    os.system('cp -r {} {}'.format(index_path, back_path))
except:
    pass


block_client = initMongo(MongoClient('mongodb://root:' + urllib.parse.quote('longhash123!@#QAZ') + '@127.0.0.1'), BLOCK)
tx_header_client = initMongo(MongoClient('mongodb://root:' + urllib.parse.quote('longhash123!@#QAZ') + '@127.0.0.1'), TX_HEADER)
tx_input_client = initMongo(MongoClient('mongodb://root:' + urllib.parse.quote('longhash123!@#QAZ') + '@127.0.0.1'), TX_INPUT)
tx_output_client = initMongo(MongoClient('mongodb://root:' + urllib.parse.quote('longhash123!@#QAZ') + '@127.0.0.1'), TX_OUTPUT)
balance_client = initMongo(MongoClient('mongodb://root:' + urllib.parse.quote('longhash123!@#QAZ') + '@127.0.0.1'), 'balance')


#filename = './parse_result_auto2/block.csv'
#line = subprocess.check_output(['tail', '-1', filename])
#start_block = int(line.decode('utf-8').split(',')[0]) + 1

start_block = 0 

for block in blockchain.get_ordered_blocks(back_path, start=start_block):
    # handle block
    block_header = block.header
    height = block.height
    # height+=1
    print(height)
    content = [height, block.hash, block_header.version, block_header.previous_block_hash, block_header.merkle_root, str(block_header.timestamp), block_header.bits, block_header.difficulty, block_header.nonce, block.n_transactions]
    #columns = ['height','hash','version','previous_block_hash', 'merkle_root', 'timestamp', 'bits', 'difficulty', 'nonce' , 'n_transactions']
    #content = [str(item) for item in content]
    with open(parser_result_path+'block.csv','a') as wf:
        insertMongo(block_client, content, block_column)
        content = [str(item) for item in content]
        wf.write(','.join(content)+'\n')
    tx_results = []
    inputs = []
    outputs = []
    #handle transactions
    for tx_index, tx in enumerate(block.transactions):
        columns = ['height', 'tx_index', 'version', 'locktime', 'hash', 'n_inputs', 'n_outputs', 'is_segwit']
        is_segwit = tx.is_segwit
        is_segwit = 1 if is_segwit else 0
        content = [height, tx_index, tx.version, tx.locktime, tx.txid, tx.n_inputs, tx.n_outputs, is_segwit]
        #content = [str(item) for item in content]
        #insert_mysql('tx_header', columns, content)
        tx_results.append(content)
        for output_num, tx_output in enumerate(tx.outputs):
            try:
                output_address = str(tx_output.addresses).split('=')[1].split(')')[0]
            except:
                output_address = None
            content = [height, tx_index, tx.txid, output_num, tx_output.value, str(output_address), str(tx_output.script), tx_output.type]
            columns = ['height', 'tx_index', 'txhash','output_index', 'value', 'addresses', 'script', 'type']
            #content = [str(item) for item in content]
            key_str = [str(item) for item in content[2:4]]
            value_str = [str(item) for item in content[4:6]]
            key = ','.join(key_str).encode()
            value = ','.join(value_str).encode()
            txhash_address.Put(key, value)
            #insert_mysql('tx_output', columns, content)
            outputs.append(content)
        for input_num, tx_input in enumerate(tx.inputs):
            witnesses = tx_input.witnesses
            witnesses = [b2a_hex(item).decode("utf-8") for item in witnesses]
            witnesses = ' '.join(witnesses)
            content = [height, tx_index, input_num, tx_input.transaction_hash, tx_input.transaction_index, tx_input.sequence_number, str(tx_input.script), witnesses]
            #columns = ['height', 'tx_index', 'input_index', 'transaction_hash', 'transaction_index', 'sequence_number', 'script', 'witnesses']
            #content = [str(item) for item in content]
            txhash = content[3]
            if txhash == '0000000000000000000000000000000000000000000000000000000000000000':
                value = ['null', 'null']
            else:
                key_str = [str(item) for item in content[3:5]]
                key = ','.join(key_str).encode()
                #print key
                try:
                    value = txhash_address.Get(key).decode().split(',')
                except:
                    value = ['none','none']
            content = content + value
            #insert_mysql('tx_input', columns, content)
            inputs.append(content)
    with open(parser_result_path+'tx_header.csv','a') as wf:
        insertMongoMany(tx_header_client, tx_results, tx_header_column)
        for tx_detail in tx_results:
            # insertMongo(tx_header_client, tx_detail, tx_header_column)
            tx_detail = [str(item) for item in tx_detail]
            wf.write(','.join(tx_detail)+'\n')
    with open(parser_result_path+'tx_input.csv','a') as wf:
        insertMongoMany(tx_input_client, inputs, tx_input_column)
        for tx_input in inputs:
            address = tx_input[-1]
            if address != 'null' and address!='None':
                try:
                    value, height = int(tx_input[-2]), int(tx_input[0])
                    update_input_balance(balance_client, address, value, height) 
                except:
                    print('Transaction input not linked!')
            tx_input = [str(item) for item in tx_input]
            wf.write(','.join(tx_input)+'\n')
    with open(parser_result_path+'tx_output.csv','a') as wf:
        insertMongoMany(tx_output_client, outputs, tx_output_column)
        for tx_output in outputs:
            address = tx_output[-3]
            if address != 'null' and address!='None':
                value, height = int(tx_output[-4]), int(tx_output[0])
                update_output_balance(balance_client, address, value, height)
            #insertMongo(tx_output_client, tx_output, tx_output_column)
            tx_output = [str(item) for item in tx_output]
            wf.write(','.join(tx_output)+'\n')
print(datetime.datetime.now() - start)
