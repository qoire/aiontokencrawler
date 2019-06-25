import os
import csv
import sqlite3

# assumes python path includes bcdbr
from bcdbr.eth import gethdb, bloom, decoding
from bcdbr.util.hashutil import keccak256

import logging

START = 4357444
FINISH = 7854631
BALANCE_DIR = 'output'
GETH_DB_PATH = "/home/yao/Ethereum/geth-linux-amd64-1.8.27-4bcc0a37/geth-storage/geth/chaindata"

OUT_DB_PATH = "/media/yao/STORAGE/aion-erc20/erc20-burnhistory.sqlite"

TRANSFER_EVENT = b"Transfer(address,address,uint256)"
TRANSFER_EVENT_HASH = keccak256(TRANSFER_EVENT)
BURN_EVENT = b"Burn(address,bytes32,uint256)"
BURN_EVENT_HASH = keccak256(BURN_EVENT)
CONTRACT_ADDR = bytes.fromhex("4CEdA7906a5Ed2179785Cd3A40A69ee8bc99C466")
LEDGER_ADDR = bytes.fromhex("D180443cFB5015088fCC6689c9D66660FC20155c")
MINTING_ADDR = bytes.fromhex("50b26685bc788e164d940f0a73770f4b9196b052")
ZERO_ADDR = bytes.fromhex("0000000000000000000000000000000000000000")
MULTIMINT_METHOD_ID = bytes.fromhex("88df13fa")

db = gethdb.create_db(GETH_DB_PATH)

def format_input(l):
    return (l.topics[1][12:], l.topics[2][12:], int.from_bytes(l.data, 'big'))

def execute_transfer(state, _from, to, amount, type, txhash):
    from_balance = 0
    to_balance = 0

    if not _from == to:
        if to in state:
            to_balance = state[to]
        state[to] = to_balance + amount

        # check invariant should always be true
        print("(%s) %s[%s] -> %s -> %s[%s] type: %s" %
            (txhash.hex(), _from.hex(), from_balance, amount, to.hex(), to_balance, type))

    return (state, (txhash, _from, from_balance, to, to_balance, amount, type))

def loop(database, i, state):
    state = state
    transfers = []

    block = gethdb.get_block_header(database, i)
    state_changed = False
    if not bloom.has_address(CONTRACT_ADDR, block.logsbloom):
        return (state, state_changed, transfers)

    block = gethdb.get_fullblock_from_num(database, i)

    # otherwise we need to check each receipt
    for tr in zip(block.transactions, block.receipts):
        tx = tr[0]
        rec = tr[1]

        is_mint = tx.payload[0:4] == MULTIMINT_METHOD_ID and tx.recipient == LEDGER_ADDR
        for l in rec.logs:

            if l.address != CONTRACT_ADDR:
                continue

            if not is_mint:
                if l.topics[0] == BURN_EVENT_HASH:
                    f = format_input(l)
                    (state, transfer) = execute_transfer(state, f[0], f[1], f[2], "burn", rec.txhash)
                    transfers.append(transfer)
                    state_changed = True
                    continue

    return (state, state_changed, transfers)

conn = sqlite3.connect(OUT_DB_PATH)
conn.isolation_level = None

# output database related functionality
def setup_database():
    # output database
    c = conn.cursor()
    try:
        c.execute("CREATE TABLE transfers (txhash text, blocknum integer, sender text, recipient text, amount text, type text)")
        c.execute("CREATE TABLE balances (address text, balance text, blocknum integer, primary key (address, blocknum))")
        c.execute("PRAGMA journal_mode = MEMORY")
        c.execute('PRAGMA synchronous=OFF')
        conn.commit()
    finally:
        c.close()

def commit_state(state, transfers, block_num):
    cf = []
    for (h, f, fb, t, tb, amount, tp) in transfers:
        cf.append((h.hex(), block_num, f.hex(), t.hex(), str(amount), tp))
    
    bf = []
    for k, v in state.items():
        if v > 0:
            bf.append((k.hex(), str(v), block_num))

    c = conn.cursor()
    try:
        c.execute("BEGIN")
        c.executemany("INSERT INTO transfers values(?,?,?,?,?,?)", cf)
        c.executemany("INSERT INTO balances values(?,?,?)", bf)
        c.execute("COMMIT")
    except e:
        c.execute("ROLLBACK")
        raise
    finally:
        c.close()

if __name__ == "__main__":
    try:
        state = {}
        setup_database()
        for i in range(START, FINISH + 1):
            (state, state_changed, transfers) = loop(db, i, state)
            if (state_changed):
                print("processed block %s/%s" % (i, FINISH))
                commit_state(state, transfers, i)
    finally:
        conn.close()
