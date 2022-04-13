#!/usr/bin/env python3
"""
Author: Aleksandra Sokolowska
for Validity Labs AG
"""

from web3 import Web3
from organize import *
import time

#uncomment one of the options below
# 1. connection via Infura
#web3 = Web3(Web3.HTTPProvider("https://mainnet.infura.io/your-personal-number"))

# 2. or connection via local node
web3 = Web3(Web3.IPCProvider('/home/ubuntu/.ethereum/geth.ipc'))

desired_contract = '0xAf5191B0De278C7286d6C7CC6ab6BB8A73bA2Cd6'

# load a block.
Nblocks = web3.eth.blockNumber - 14402855
output_every = 2
start_time = time.time()
try:
    with open('lastblock.txt', 'r') as f:
        start = int(f.read())+1
except FileNotFoundError:
    start = 14402855

#define tables that will go to the SQLite database
table_quick = []
table_tx = []
table_block = []

count = 0
#loop over all blocks
for block in range(start, start+Nblocks):

    block_table, block_data = order_table_block(block,web3)
    #list of block data that will go to the DB
    table_block.append(block_table)
    print("After Block")
    #all transactions on the block
    for hashh in block_data['transactions']:
        #print(web3.toHex(hashh))
        tx_data = web3.eth.getTransaction(hashh)


        if tx_data['to'] == desired_contract:
            quick_table, tx_data = order_table_quick(hashh,block, web3)
            table_quick.append(quick_table)

            #list of tx data that will go to the DB
            TX_table = order_table_tx(tx_data,hashh, web3)
            table_tx.append(TX_table)
    count = count + 1
    print("After Txn")
    #print(count)
    #dump output every 2 blocks
    if (count % output_every) == 0:
        execute_sql(table_quick, table_tx, table_block)

        #free up memory
        del table_quick
        del table_tx
        del table_block
        table_quick = []
        table_tx = []
        table_block = []

        #update the current block number to a file
        with open('lastblock.txt', 'w') as f:
            f.write("%d" % block)
    if (count % 10) == 0:
        end = time.time()
        with open('timeperXblocks.txt', 'a') as f:
            f.write("%d %f \n" % (block, end-start_time))
    if (count % 100) == 0:
        print("100 new blocks completed.")
