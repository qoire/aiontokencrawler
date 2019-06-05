import os
import csv

# assumes python path includes bcdbr
from bcdbr.eth import gethdb, bloom, decoding
from bcdbr.util.hashutil import keccak256

import logging

# some logging setup
FORMAT = '%{asctime}-15s %{message}s'
logging.basicConfig(format=FORMAT)
log = logging.getLogger('crawler')

START = 4357444
FINISH = 7854631
BALANCE_DIR = 'output'
GETH_DB_PATH = "/home/yao/Ethereum/geth-linux-amd64-1.8.27-4bcc0a37/geth-storage/geth/chaindata"

TRANSFER_EVENT = b"Transfer(address,address,uint256)"
TRANSFER_EVENT_HASH = keccak256(TRANSFER_EVENT)
BURN_EVENT = b"Burn(address,address,uint256)"
BURN_EVENT_HASH = keccak256(BURN_EVENT)
CONTRACT_ADDR = bytes.fromhex("4CEdA7906a5Ed2179785Cd3A40A69ee8bc99C466")
LEDGER_ADDR = bytes.fromhex("D180443cFB5015088fCC6689c9D66660FC20155c")
MINTING_ADDR = bytes.fromhex("50b26685bc788e164d940f0a73770f4b9196b052")
MULTIMINT_METHOD_ID = bytes.fromhex("88df13fa")

db = gethdb.create_db(GETH_DB_PATH)

def format_input(l):
    return (l.topics[1][12:], l.topics[2][12:], int.from_bytes(l.data, 'big'))

def execute_transfer(state, _from, to, amount, is_mint, txhash):
    from_balance = 0
    to_balance = 0

    if not _from == to:
        if _from in state:
            from_balance = state[_from]

        if to in state:
            to_balance = state[to]

        state[to] = to_balance + amount
        if not is_mint:
            state[_from] = from_balance - amount

    # check invariant should always be true
    print("(%s) %s[%s] -> %s[%s] amount: %s mint: %s" %
        (txhash.hex(), _from.hex(), from_balance, to.hex(), to_balance, amount, is_mint))
    if _from in state:
        assert state[_from] >= 0
    return state

def loop(database, i, state):
    state = state
    block = gethdb.get_fullblock_from_num(database, i)

    if not bloom.has_address(CONTRACT_ADDR, block.logsbloom):
        return state

    # otherwise we need to check each receipt
    for tr in zip(block.transactions, block.receipts):
        tx = tr[0]
        rec = tr[1]

        is_mint = tx.payload[0:4] == MULTIMINT_METHOD_ID and tx.recipient == LEDGER_ADDR
        for l in rec.logs:

            if l.address != CONTRACT_ADDR:
                continue

            if not is_mint:
                if l.topics[0] == TRANSFER_EVENT_HASH or l.topics[0] == BURN_EVENT_HASH:
                    f = format_input(l)
                    state = execute_transfer(state, f[0], f[1], f[2], False, rec.txhash)
            else:
                # heuristic: the mint function only ever calls the contract
                # we assume that if the transaction directly calls the contract
                # and something is transferred (on the logs), then it must
                # be a mint... (any counterexamples to this?)
                if (l.topics[0] == TRANSFER_EVENT_HASH):
                    f = format_input(l)
                    state = execute_transfer(state, f[0], f[1], f[2], True, rec.txhash)
    return state

def flushfile(state, block_num):
    with open("%s/%s.csv" % (BALANCE_DIR, block_num), 'w') as f:
        w = csv.writer(f)
        for k, v in state.items():
            w.writerow([k.hex(), v])

# flush directories
if os.path.exists(BALANCE_DIR):
    os.removedirs(BALANCE_DIR)
os.makedirs(BALANCE_DIR)

if __name__ == "__main__":
    state = {}
    for i in range(START, FINISH + 1):
        state = loop(db, i, state)
        if (i % 10000 == 0 or i == FINISH):
            flushfile(state, i)
            print("block %s output", i)
