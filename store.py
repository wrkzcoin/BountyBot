from typing import List, Dict
from datetime import datetime
import time
import json
import asyncio

import daemonrpc_client, rpc_client, wallet, walletapi, addressvalidation
from config import config
import sys, traceback
import os.path

# MySQL
import pymysql, pymysqlpool
import pymysql.cursors

# redis
import redis
redis_pool = None
redis_conn = None
redis_expired = 120

FEE_PER_BYTE_COIN = config.Fee_Per_Byte_Coin.split(",")
XS_COIN = [""]

pymysqlpool.logger.setLevel('DEBUG')
myconfig = {
    'host': config.mysql.host,
    'user':config.mysql.user,
    'password':config.mysql.password,
    'database':config.mysql.db,
    'charset':'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit':True
    }

connPool = pymysqlpool.ConnectionPool(size=4, name='connPool', **myconfig)
conn = connPool.get_connection(timeout=5, retry_num=2)

sys.path.append("..")

ENABLE_COIN = config.Enable_Coin.split(",")

# Coin using wallet-api
WALLET_API_COIN = config.Enable_Coin_WalletApi.split(",")

def init():
    global redis_pool
    print("PID %d: initializing redis pool..." % os.getpid())
    redis_pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True, db=10)


def openRedis():
    global redis_pool, redis_conn
    if redis_conn is None:
        try:
            redis_conn = redis.Redis(connection_pool=redis_pool)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)


# connPool 
def openConnection():
    global conn, connPool
    try:
        if conn is None:
            conn = connPool.get_connection(timeout=5, retry_num=2)
    except:
        print("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()


async def sql_register_user(userID, coin: str, user_server: str, chat_id: int = 0):
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    if user_server == "TELEGRAM" and chat_id == 0:
        return

    global conn
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = None
            result = None
            if coin_family == "TRTL":
                sql = """ SELECT * FROM cn_user_paymentid WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """
                cur.execute(sql, (userID, COIN_NAME, user_server))
                result = cur.fetchone()
            elif coin_family == "XMR":
                sql = """ SELECT * FROM xmr_user_paymentid WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """
                cur.execute(sql, (str(userID), COIN_NAME, user_server))
                result = cur.fetchone()
            elif coin_family == "DOGE":
                sql = """ SELECT * FROM doge_user WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """
                cur.execute(sql, (str(userID), COIN_NAME, user_server))
                result = cur.fetchone()
            if result is None:
                balance_address = {}
                main_address = getattr(getattr(config,"coin"+COIN_NAME),"MainAddress") if coin_family != "DOGE" else None
                if coin_family == "XMR":
                    balance_address = await wallet.make_integrated_address_xmr(main_address, COIN_NAME)
                    sql = """ INSERT INTO xmr_user_paymentid (`coin_name`, `user_id`, `main_address`, `paymentid`, 
                              `int_address`, `paymentid_ts`, `user_server`, `chat_id`) 
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
                    cur.execute(sql, (COIN_NAME, str(userID), main_address, balance_address['payment_id'], 
                                balance_address['integrated_address'], int(time.time()), user_server, chat_id))
                    conn.commit()
                elif coin_family == "TRTL":
                    balance_address['payment_id'] = addressvalidation.paymentid()
                    balance_address['integrated_address'] = addressvalidation.make_integrated_cn(main_address, COIN_NAME, balance_address['payment_id'])['integrated_address']
                    sql = """ INSERT INTO cn_user_paymentid (`coin_name`, `user_id`, `main_address`, `paymentid`, 
                              `int_address`, `paymentid_ts`, `user_server`, `chat_id`) 
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
                    cur.execute(sql, (COIN_NAME, str(userID), main_address, balance_address['payment_id'], 
                                balance_address['integrated_address'], int(time.time()), user_server, chat_id))
                    conn.commit()
                elif coin_family == "DOGE":
                    user_address = await wallet.doge_register(str(userID), COIN_NAME)
                    sql = """ INSERT INTO doge_user (`coin_name`, `user_id`, `address`, `address_ts`, 
                              `privateKey`, `user_server`, `chat_id`) 
                              VALUES (%s, %s, %s, %s, %s, %s, %s) """
                    cur.execute(sql, (COIN_NAME, str(userID), user_address['address'], int(time.time()), user_address['privateKey'], user_server, chat_id))
                    balance_address['address'] = user_address['address']
                    conn.commit()
                return balance_address
            else:
                return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)

        
async def sql_get_userwallet(userID, coin: str, user_server: str = 'DISCORD'):
    global conn, redis_conn, redis_expired
    COIN_NAME = coin.upper()
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    try:
        openConnection()
        with conn.cursor() as cur:
            result = None
            if coin_family == "TRTL":
                sql = """ SELECT * FROM cn_user_paymentid 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server` = %s LIMIT 1 """
                cur.execute(sql, (str(userID), COIN_NAME, user_server))
                result = cur.fetchone()
            elif coin_family == "XMR":
                sql = """ SELECT * FROM xmr_user_paymentid 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server` = %s LIMIT 1 """
                cur.execute(sql, (str(userID), COIN_NAME, user_server))
                result = cur.fetchone()
            elif coin_family == "DOGE":
                sql = """ SELECT * FROM doge_user 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server` = %s LIMIT 1 """
                cur.execute(sql, (str(userID), COIN_NAME, user_server))
                result = cur.fetchone()
            if result:
                userwallet = result
                if coin_family == "XMR" or coin_family == "TRTL":
                    userwallet['balance_wallet_address'] = userwallet['int_address']
                if coin_family == "DOGE":
                    userwallet['balance_wallet_address'] = userwallet['address']
                return userwallet
            else:
                return None
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


async def sql_update_user(userID, user_wallet_address, coin: str, user_server: str = 'DISCORD'):
    global redis_conn
    COIN_NAME = coin.upper()
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if coin_family == "TRTL":
                sql = """ UPDATE cn_user_paymentid SET user_wallet_address=%s WHERE user_id=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """               
                cur.execute(sql, (user_wallet_address, str(userID), COIN_NAME, user_server))
                conn.commit()
            elif coin_family == "XMR":
                sql = """ UPDATE xmr_user_paymentid SET user_wallet_address=%s WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """               
                cur.execute(sql, (user_wallet_address, str(userID), COIN_NAME, user_server))
                conn.commit()
            elif coin_family == "DOGE":
                sql = """ UPDATE doge_user SET user_wallet_address=%s WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """               
                cur.execute(sql, (user_wallet_address, str(userID), COIN_NAME, user_server))
                conn.commit()
            return user_wallet_address  # return userwallet
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


async def sql_update_balances(coin: str):
    global conn, redis_conn, redis_expired, XS_COIN
    updateTime = int(time.time())
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")

    gettopblock = None
    timeout = 12
    try:
        if COIN_NAME not in ["DOGE", "LTC", "BTC", "DASH", "BCH"]:
            gettopblock = await daemonrpc_client.gettopblock(COIN_NAME, time_out=timeout)
        else:
            gettopblock = await rpc_client.call_doge('getblockchaininfo', COIN_NAME)
    except asyncio.TimeoutError:
        pass
    except Exception as e:
        traceback.print_exc(file=sys.stdout)

    height = None
    if gettopblock:
        if coin_family == "TRTL" or coin_family == "XMR":
            height = int(gettopblock['block_header']['height'])
        elif coin_family == "DOGE":
            height = int(gettopblock['blocks'])
        # store in redis
        try:
            openRedis()
            if redis_conn:
                redis_conn.set(f'CryptoBountyBot:DAEMON_HEIGHT_{COIN_NAME}', str(height))
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
    else:
        try:
            openRedis()
            if redis_conn and redis_conn.exists(f'CryptoBountyBot:DAEMON_HEIGHT_{COIN_NAME}'):
                height = int(redis_conn.get(f'CryptoBountyBot:DAEMON_HEIGHT_{COIN_NAME}'))
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    if coin_family == "TRTL" and COIN_NAME not in XS_COIN:
        print('SQL: Updating get_transfers '+COIN_NAME)
        get_transfers = await walletapi.get_transfers_cn(COIN_NAME)
        if len(get_transfers) >= 1:
            try:
                openConnection()
                with conn.cursor() as cur:
                    sql = """ SELECT * FROM cn_get_transfers WHERE `coin_name` = %s """
                    cur.execute(sql, (COIN_NAME,))
                    result = cur.fetchall()
                    d = [i['txid'] for i in result]
                    # print('=================='+COIN_NAME+'===========')
                    # print(d)
                    # print('=================='+COIN_NAME+'===========')
                    list_balance_user = {}
                    for tx in get_transfers:
                        # add to balance only confirmation depth meet
                        if height > int(tx['blockHeight']) + wallet.get_confirm_depth(COIN_NAME):
                            if ('paymentID' in tx) and (tx['paymentID'] in list_balance_user):
                                if tx['transfers'][0]['amount'] > 0:
                                    list_balance_user[tx['paymentID']] += tx['transfers'][0]['amount']
                            elif ('paymentID' in tx) and (tx['paymentID'] not in list_balance_user):
                                if tx['transfers'][0]['amount'] > 0:
                                    list_balance_user[tx['paymentID']] = tx['transfers'][0]['amount']
                            try:
                                if tx['hash'] not in d:
                                    sql = """ INSERT IGNORE INTO cn_get_transfers (`coin_name`, `txid`, 
                                    `payment_id`, `height`, `timestamp`, `amount`, `fee`, `decimal`, `address`, time_insert) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
                                    cur.execute(sql, (COIN_NAME, tx['hash'], tx['paymentID'], tx['blockHeight'], tx['timestamp'],
                                                      tx['transfers'][0]['amount'], tx['fee'], wallet.get_decimal(COIN_NAME), tx['transfers'][0]['address'], int(time.time())))
                                    conn.commit()
                                    # add to notification list also
                                    sql = """ INSERT IGNORE INTO notify_new_tx (`coin_name`, `txid`, 
                                    `payment_id`, `height`, `amount`, `fee`, `decimal`) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s) """
                                    cur.execute(sql, (COIN_NAME, tx['hash'], tx['paymentID'], tx['blockHeight'],
                                                      tx['transfers'][0]['amount'], tx['fee'], wallet.get_decimal(COIN_NAME)))
                                    conn.commit()
                            except pymysql.err.Warning as e:
                                print(e)
                            except Exception as e:
                                traceback.print_exc(file=sys.stdout)
                    if len(list_balance_user) > 0:
                        list_update = []
                        timestamp = int(time.time())
                        for key, value in list_balance_user.items():
                            list_update.append((value, timestamp, key))
                        cur.executemany(""" UPDATE cn_user_paymentid SET `actual_balance` = %s, `lastUpdate` = %s 
                                        WHERE paymentid = %s """, list_update)
                        conn.commit()
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
    if coin_family == "TRTL" and (COIN_NAME in XS_COIN):
        print('SQL: Updating get_transfers '+COIN_NAME)
        get_transfers = await wallet.getTransactions(COIN_NAME, int(height)-100000, 100000)
        try:
            if len(get_transfers) >= 1:
                openConnection()
                with conn.cursor() as cur:
                    sql = """ SELECT * FROM cn_get_transfers WHERE `coin_name` = %s """
                    cur.execute(sql, (COIN_NAME,))
                    result = cur.fetchall()
                    d = [i['txid'] for i in result]
                    # print('=================='+COIN_NAME+'===========')
                    # print(d)
                    # print('=================='+COIN_NAME+'===========')
                    list_balance_user = {}
                    for txes in get_transfers:
                        tx_in_block = txes['transactions']
                        for tx in tx_in_block:
                            # Could be one block has two or more tx with different payment ID
                            # add to balance only confirmation depth meet
                            if height > int(tx['blockIndex']) + wallet.get_confirm_depth(COIN_NAME):
                                if ('paymentId' in tx) and (tx['paymentId'] in list_balance_user):
                                    if tx['amount'] > 0:
                                        list_balance_user[tx['paymentId']] += tx['amount']
                                elif ('paymentId' in tx) and (tx['paymentId'] not in list_balance_user):
                                    if tx['amount'] > 0:
                                        list_balance_user[tx['paymentId']] = tx['amount']
                                try:
                                    if tx['transactionHash'] not in d:
                                        addresses = tx['transfers']
                                        address = ''
                                        for each_add in addresses:
                                            if len(each_add['address']) > 0: address = each_add['address']
                                            break
                                            
                                        sql = """ INSERT IGNORE INTO cn_get_transfers (`coin_name`, `txid`, 
                                        `payment_id`, `height`, `timestamp`, `amount`, `fee`, `decimal`, `address`, time_insert) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
                                        cur.execute(sql, (COIN_NAME, tx['transactionHash'], tx['paymentId'], tx['blockIndex'], tx['timestamp'],
                                                          tx['amount'], tx['fee'], wallet.get_decimal(COIN_NAME), address, int(time.time())))
                                        conn.commit()
                                        # add to notification list also
                                        sql = """ INSERT IGNORE INTO notify_new_tx (`coin_name`, `txid`, 
                                        `payment_id`, `height`, `amount`, `fee`, `decimal`) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s) """
                                        cur.execute(sql, (COIN_NAME, tx['transactionHash'], tx['paymentId'], tx['blockIndex'],
                                                          tx['amount'], tx['fee'], wallet.get_decimal(COIN_NAME)))
                                        conn.commit()
                                except pymysql.err.Warning as e:
                                    print(e)
                                except Exception as e:
                                    traceback.print_exc(file=sys.stdout)
                            else:
                                print('{} has some tx but not yet meet confirmation depth.'.format(COIN_NAME))
            if list_balance_user and len(list_balance_user) >= 1:
                openConnection()
                with conn.cursor() as cur:
                    sql = """ SELECT coin_name, payment_id, SUM(amount) AS txIn FROM cn_get_transfers 
                              WHERE coin_name = %s AND amount > 0 
                              GROUP BY payment_id """
                    cur.execute(sql, (COIN_NAME,))
                    result = cur.fetchall()
                    timestamp = int(time.time())
                    list_update = []
                    if result and len(result) > 0:
                        for eachTxIn in result:
                            list_update.append((eachTxIn['txIn'], timestamp, eachTxIn['payment_id']))
                        cur.executemany(""" UPDATE cn_user_paymentid SET `actual_balance` = %s, `lastUpdate` = %s 
                                        WHERE paymentid = %s """, list_update)
                        conn.commit()
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
    elif coin_family == "XMR":
        print('SQL: Updating get_transfers '+COIN_NAME)
        get_transfers = await wallet.get_transfers_xmr(COIN_NAME)
        if len(get_transfers) >= 1:
            try:
                openConnection()
                with conn.cursor() as cur:
                    sql = """ SELECT * FROM xmr_get_transfers WHERE `coin_name` = %s """
                    cur.execute(sql, (COIN_NAME,))
                    result = cur.fetchall()
                    d = [i['txid'] for i in result]
                    # print('=================='+COIN_NAME+'===========')
                    # print(d)
                    # print('=================='+COIN_NAME+'===========')
                    list_balance_user = {}
                    for tx in get_transfers['in']:
                        # add to balance only confirmation depth meet
                        if height > int(tx['height']) + wallet.get_confirm_depth(COIN_NAME):
                            if ('payment_id' in tx) and (tx['payment_id'] in list_balance_user):
                                list_balance_user[tx['payment_id']] += tx['amount']
                            elif ('payment_id' in tx) and (tx['payment_id'] not in list_balance_user):
                                list_balance_user[tx['payment_id']] = tx['amount']
                            try:
                                if tx['txid'] not in d:
                                    sql = """ INSERT IGNORE INTO xmr_get_transfers (`coin_name`, `in_out`, `txid`, 
                                    `payment_id`, `height`, `timestamp`, `amount`, `fee`, `decimal`, `address`, time_insert) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
                                    cur.execute(sql, (COIN_NAME, tx['type'].upper(), tx['txid'], tx['payment_id'], tx['height'], tx['timestamp'],
                                                      tx['amount'], tx['fee'], wallet.get_decimal(COIN_NAME), tx['address'], int(time.time())))
                                    conn.commit()
                                    # add to notification list also
                                    sql = """ INSERT IGNORE INTO notify_new_tx (`coin_name`, `txid`, 
                                    `payment_id`, `height`, `amount`, `fee`, `decimal`) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s) """
                                    cur.execute(sql, (COIN_NAME, tx['txid'], tx['payment_id'], tx['height'],
                                                      tx['amount'], tx['fee'], wallet.get_decimal(COIN_NAME)))
                            except pymysql.err.Warning as e:
                                print(e)
                            except Exception as e:
                                traceback.print_exc(file=sys.stdout)
                    if len(list_balance_user) > 0:
                        list_update = []
                        timestamp = int(time.time())
                        for key, value in list_balance_user.items():
                            list_update.append((value, timestamp, key))
                        cur.executemany(""" UPDATE xmr_user_paymentid SET `actual_balance` = %s, `lastUpdate` = %s 
                                        WHERE paymentid = %s """, list_update)
                        conn.commit()
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
    elif coin_family == "DOGE":
        print('SQL: Updating get_transfers '+COIN_NAME)
        get_transfers = await wallet.doge_listtransactions(COIN_NAME)
        if get_transfers and len(get_transfers) >= 1:
            try:
                openConnection()
                with conn.cursor() as cur:
                    sql = """ SELECT * FROM doge_get_transfers WHERE `coin_name` = %s AND `category` IN (%s, %s) """
                    cur.execute(sql, (COIN_NAME, 'receive', 'send'))
                    result = cur.fetchall()
                    d = [i['txid'] for i in result]
                    # print('=================='+COIN_NAME+'===========')
                    # print(d)
                    # print('=================='+COIN_NAME+'===========')
                    list_balance_user = {}
                    for tx in get_transfers:
                        # add to balance only confirmation depth meet
                        if wallet.get_confirm_depth(COIN_NAME) < int(tx['confirmations']):
                            if ('address' in tx) and (tx['address'] in list_balance_user) and (tx['amount'] > 0):
                                list_balance_user[tx['address']] += tx['amount']
                            elif ('address' in tx) and (tx['address'] not in list_balance_user) and (tx['amount'] > 0):
                                list_balance_user[tx['address']] = tx['amount']
                            try:
                                if tx['txid'] not in d:
                                    if tx['category'] == "receive":
                                        sql = """ INSERT IGNORE INTO doge_get_transfers (`coin_name`, `txid`, `blockhash`, 
                                        `address`, `blocktime`, `amount`, `confirmations`, `category`, `time_insert`) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
                                        cur.execute(sql, (COIN_NAME, tx['txid'], tx['blockhash'], tx['address'],
                                                          tx['blocktime'], tx['amount'], tx['confirmations'], tx['category'], int(time.time())))
                                        conn.commit()
                                    # add to notification list also, doge payment_id = address
                                    if (tx['amount'] > 0) and tx['category'] == 'receive':
                                        sql = """ INSERT IGNORE INTO notify_new_tx (`coin_name`, `txid`, 
                                        `payment_id`, `blockhash`, `amount`, `decimal`) 
                                        VALUES (%s, %s, %s, %s, %s, %s) """
                                        cur.execute(sql, (COIN_NAME, tx['txid'], tx['address'], tx['blockhash'],
                                                          tx['amount'], wallet.get_decimal(COIN_NAME)))
                            except pymysql.err.Warning as e:
                                print(e)
                            except Exception as e:
                                traceback.print_exc(file=sys.stdout)
                    if len(list_balance_user) > 0:
                        list_update = []
                        timestamp = int(time.time())
                        for key, value in list_balance_user.items():
                            list_update.append((value, timestamp, key))
                        cur.executemany(""" UPDATE doge_user SET `actual_balance` = %s, `lastUpdate` = %s 
                                        WHERE address = %s """, list_update)
                        conn.commit()
            except Exception as e:
                traceback.print_exc(file=sys.stdout)


def sql_user_balance(userID: str, coin: str, user_server: str = 'DISCORD'):
    global conn, redis_conn, redis_expired
    COIN_NAME = coin.upper()
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    try:
        openConnection()
        with conn.cursor() as cur:
            # Credit by admin is positive (Positive)
            sql = """ SELECT SUM(amount) AS Credited FROM credit_balance 
                      WHERE `coin_name`=%s AND `to_userid`=%s AND `user_server`=%s """
            cur.execute(sql, (COIN_NAME, userID, user_server))
            result = cur.fetchone()
            if result:
                Credited = result['Credited']
            else:
                Credited = 0

            # When sending tx out, (negative)
            sql = ""
            if coin_family == "TRTL":
                sql = """ SELECT SUM(amount) AS SendingOut FROM cn_external_tx 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s """
            elif coin_family == "XMR":
                sql = """ SELECT SUM(amount) AS SendingOut FROM xmr_external_tx 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s """
            elif coin_family == "DOGE":
                sql = """ SELECT SUM(amount) AS SendingOut FROM doge_external_tx 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s """
            cur.execute(sql, (userID, COIN_NAME, user_server))
            result = cur.fetchone()
            if result:
                SendingOut = result['SendingOut']
            else:
                SendingOut = 0

            # When sending tx out, user needs to pay for tx as well (negative)
            sql = ""
            if coin_family == "TRTL":
                sql = """ SELECT SUM(fee) AS FeeExpense FROM cn_external_tx 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s """
            elif coin_family == "XMR":
                sql = """ SELECT SUM(fee) AS FeeExpense FROM xmr_external_tx 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s """
            elif coin_family == "DOGE":
                sql = """ SELECT SUM(fee) AS FeeExpense FROM doge_external_tx 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s """
            cur.execute(sql, (userID, COIN_NAME, user_server))
            result = cur.fetchone()
            if result:
                FeeExpense = result['FeeExpense']
            else:
                FeeExpense = 0

        OnBounty = 0 # TODO: DELETE
        balance = {}
        balance['Adjust'] = 0
        balance['Credited'] = float(Credited) if Credited else 0
        balance['SendingOut'] = float(SendingOut) if SendingOut else 0
        balance['FeeExpense'] = float(FeeExpense) if FeeExpense else 0
        balance['Adjust'] = balance['Credited'] - balance['SendingOut'] - balance['FeeExpense']
        balance['OnBounty'] = float(OnBounty) if OnBounty else 0 # TODO another bounty table
        #print(COIN_NAME)
        #print(balance)
        return balance
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def sql_cn_xmr_credit(user_from: str, to_user: str, amount: float, coin: str, reason: str, user_server: str = 'DISCORD'):
    global conn
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    COIN_NAME = coin.upper()
    try:
        openConnection()
        with conn.cursor() as cur: 
            sql = """ INSERT INTO credit_balance (`coin_name`, `from_userid`, `to_userid`, `amount`, `decimal`, `credit_date`, `reason`, `user_server`) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
            cur.execute(sql, (COIN_NAME, user_from, to_user, amount, wallet.get_decimal(COIN_NAME), int(time.time()), reason, user_server))
            conn.commit()
        return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_external_cn_xmr_single(user_server: str, user_from: str, amount: float, to_address: str, coin: str, paymentid: str = None):
    global conn, XS_COIN
    COIN_NAME = coin.upper()
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    try:
        openConnection()
        tx_hash = None
        if coin_family == "XMR":
            tx_hash = await wallet.send_transaction('BOUNTYBOT', to_address, 
                                                    amount, COIN_NAME, 0)
            if tx_hash:
                with conn.cursor() as cur: 
                    sql = """ INSERT INTO xmr_external_tx (`coin_name`, `user_id`, `amount`, `fee`, `decimal`, `to_address`, 
                              `date`, `tx_hash`, `tx_key`, `user_server`) 
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
                    cur.execute(sql, (COIN_NAME, user_from, amount, tx_hash['fee'], wallet.get_decimal(COIN_NAME), to_address, 
                    int(time.time()), tx_hash['tx_hash'], tx_hash['tx_key'], user_server))
                    conn.commit()
            return tx_hash
        elif coin_family == "TRTL"and COIN_NAME not in XS_COIN:
            from_address = wallet.get_main_address(COIN_NAME)
            tx_hash = await walletapi.walletapi_send_transaction(from_address, to_address, 
                                                                 amount, COIN_NAME)
            if tx_hash:
                with conn.cursor() as cur: 
                    sql = """ INSERT INTO cn_external_tx (`coin_name`, `user_id`, `amount`, `fee`, `decimal`, `to_address`, 
                              `date`, `tx_hash`, `user_server`) 
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
                    cur.execute(sql, (COIN_NAME, user_from, amount, tx_hash['fee'], wallet.get_decimal(COIN_NAME), to_address, 
                                int(time.time()), tx_hash['transactionHash'], user_server))
                    conn.commit()
            return tx_hash
        elif coin_family == "TRTL" and COIN_NAME in XS_COIN:
            # TODO: check fee
            from_address = wallet.get_main_address(COIN_NAME)
            tx_fee = wallet.get_tx_fee(COIN_NAME)
            tx_hash = await wallet.send_transaction(from_address, to_address, 
                                                    amount, COIN_NAME)
            if tx_hash:
                with conn.cursor() as cur: 
                    sql = """ INSERT INTO cn_external_tx (`coin_name`, `user_id`, `amount`, `fee`, `decimal`, `to_address`, 
                              `date`, `tx_hash`, `user_server`) 
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
                    cur.execute(sql, (COIN_NAME, user_from, amount, tx_fee, wallet.get_decimal(COIN_NAME), to_address, 
                                int(time.time()), tx_hash['transactionHash'], user_server))
                    conn.commit()
            return tx_hash
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_external_doge(user_server: str, user_from: str, amount: float, fee: float, to_address: str, coin: str):
    global conn
    COIN_NAME = coin.upper()
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    try:
        openConnection()
        print("DOGE EXTERNAL: ")
        print((to_address, amount, user_from, COIN_NAME))
        txHash = await wallet.doge_sendtoaddress(to_address, amount, user_from, COIN_NAME)
        print("COMPLETE DOGE EXTERNAL TX")
        with conn.cursor() as cur: 
            sql = """ INSERT INTO doge_external_tx (`coin_name`, `user_id`, `amount`, `fee`, `to_address`, 
                      `date`, `tx_hash`, `user_server`) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
            cur.execute(sql, (COIN_NAME, user_from, amount, fee, to_address, int(time.time()), txHash, user_server))
            conn.commit()
        return txHash
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_get_userwallet_by_paymentid(paymentid: str, coin: str, user_server: str = 'DISCORD'):
    global conn
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    try:
        openConnection()
        with conn.cursor() as cur:
            result = None
            if coin_family == "TRTL":
                sql = """ SELECT * FROM cn_user_paymentid 
                          WHERE `paymentid`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """
                cur.execute(sql, (paymentid, COIN_NAME, user_server))
                result = cur.fetchone()
            elif coin_family == "XMR":
                sql = """ SELECT * FROM xmr_user_paymentid 
                          WHERE `paymentid`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """
                cur.execute(sql, (paymentid, COIN_NAME, user_server))
                result = cur.fetchone()
            elif coin_family == "DOGE":
                # if doge family, address is paymentid
                sql = """ SELECT * FROM doge_user 
                          WHERE `address`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """
                cur.execute(sql, (paymentid, COIN_NAME, user_server))
                result = cur.fetchone()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


async def sql_get_new_tx_table(notified: str = 'NO', failed_notify: str = 'NO'):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT * FROM notify_new_tx WHERE `notified`=%s AND `failed_notify`=%s """
            cur.execute(sql, (notified, failed_notify,))
            result = cur.fetchall()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


async def sql_update_notify_tx_table(payment_id: str, owner_id: str, owner_name: str, notified: str = 'YES', failed_notify: str = 'NO'):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ UPDATE notify_new_tx SET `owner_id`=%s, `owner_name`=%s, `notified`=%s, `failed_notify`=%s, 
                      `notified_time`=%s WHERE `payment_id`=%s """
            cur.execute(sql, (owner_id, owner_name, notified, failed_notify, float("%.3f" % time.time()), payment_id,))
            conn.commit()
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_get_deposit_alluser(user: str = 'ALL', coin: str = 'ANY'):
    global conn
    COIN_NAME = coin.upper()
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT * FROM cn_get_transfers """
            has_userall = True
            if user != 'ALL':
                sql += """ WHERE `user_id`='"""+user+"""' """
                has_userall = False
            if COIN_NAME != 'ANY':
                if has_userall:
                    sql += """ WHERE `coin_name`='"""+COIN_NAME+"""' """
                else:
                    sql += """ AND `coin_name`='"""+COIN_NAME+"""' """
            cur.execute(sql,)
            result1 = cur.fetchall()

            sql = """ SELECT * FROM xmr_get_transfers """
            has_userall = True
            if user != 'ALL':
                sql += """ WHERE `user_id`='"""+user+"""' """
                has_userall = False
            if COIN_NAME != 'ANY':
                if has_userall:
                    sql += """ WHERE `coin_name`='"""+COIN_NAME+"""' """
                else:
                    sql += """ AND `coin_name`='"""+COIN_NAME+"""' """
            cur.execute(sql,)
            result2 = cur.fetchall()

            sql = """ SELECT * FROM doge_get_transfers """
            has_userall = True
            if user != 'ALL':
                sql += """ WHERE `user_id`='"""+user+"""' """
                has_userall = False
            if COIN_NAME != 'ANY':
                if has_userall:
                    sql += """ WHERE `coin_name`='"""+COIN_NAME+"""' """
                else:
                    sql += """ AND `coin_name`='"""+COIN_NAME+"""' """
            cur.execute(sql,)
            result3 = cur.fetchall()

            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


def sql_add_logs_tx(list_tx):
    if len(list_tx) == 0:
        return 0
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ INSERT IGNORE INTO `action_tx_logs` (`uuid`, `action`, `user_id`, `user_name`, 
                      `event_date`, `msg_content`, `user_server`, `end_point`)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
            cur.executemany(sql, list_tx)
            conn.commit()
            return cur.rowcount
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def sql_info_by_server(server_id: str):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur: 
            sql = """ SELECT * FROM discord_server WHERE serverid = %s LIMIT 1 """
            cur.execute(sql, (server_id,))
            result = cur.fetchone()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def sql_addinfo_by_server(server_id: str, servername: str, prefix: str, rejoin: bool = True):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if rejoin:
                sql = """ INSERT INTO `discord_server` (`serverid`, `servername`, `prefix`)
                          VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE 
                          `servername` = %s, `prefix` = %s, `status` = %s """
                cur.execute(sql, (server_id, servername[:28], prefix, servername[:28], prefix, "REJOINED", ))
                conn.commit()
            else:
                sql = """ INSERT INTO `discord_server` (`serverid`, `servername`, `prefix`)
                          VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE 
                          `servername` = %s, `prefix` = %s"""
                cur.execute(sql, (server_id, servername[:28], prefix, servername[:28], prefix))
                conn.commit()
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def sql_changeinfo_by_server(server_id: str, what: str, value: str):
    global conn
    if what.lower() in ["servername", "prefix", "numb_user", "numb_bot", "numb_channel", "lastUpdate"]:
        try:
            openConnection()
            with conn.cursor() as cur:
                sql = """ UPDATE discord_server SET `""" + what.lower() + """` = %s WHERE `serverid` = %s """
                cur.execute(sql, (value, server_id,))
                conn.commit()
        except Exception as e:
            traceback.print_exc(file=sys.stdout)


def sql_bounty_add_data(bounty_number: int, coin_name: str, ref_id: str, bounty_amount: float, bounty_amount_after_fee: float, bounty_coin_decimal: int, 
    userid_create: str, bounty_title: str, bounty_obj: str, bounty_desc: str, bounty_type: str, 
    status: str, created_user_server: str='DISCORD'):
    global conn
    user_server = created_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ INSERT INTO `bounty_data` (`ref_id`, `bounty_amount`, `bounty_amount_after_fee`, `bounty_coin_decimal`, 
                      `userid_create`, `created_date`, `bounty_title`, `bounty_obj`, `bounty_desc`, `bounty_type`, `status`, 
                      `created_user_server`, `coin_name`, `bounty_number`)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            cur.execute(sql, (ref_id, bounty_amount, bounty_amount_after_fee, bounty_coin_decimal, userid_create,
                              int(time.time()), bounty_title, bounty_obj, bounty_desc, bounty_type, status, user_server, coin_name, bounty_number))
            conn.commit()
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


def sql_bounty_list_of_user(userid: str, status:str, created_user_server: str='DISCORD', limit: int=100):
    user_server = created_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    status = status.upper()
    if status not in ['COMPLETED','OPENED','ONGOING','CANCELLED', 'ALL']:
        return
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if status != 'ALL':
                if userid == 'ALL':
                    sql = """ SELECT * FROM bounty_data WHERE `status` = %s AND `created_user_server`=%s 
                              ORDER BY `created_date` DESC LIMIT """+str(limit)+""" """
                    cur.execute(sql, (status, user_server))
                    result = cur.fetchall()
                    return result
                else:
                    sql = """ SELECT * FROM bounty_data WHERE `status` = %s AND `created_user_server`=%s AND `userid_create`=%s 
                              ORDER BY `created_date` DESC LIMIT """+str(limit)+""" """
                    cur.execute(sql, (status, user_server, userid))
                    result = cur.fetchall()
                    return result
            else:
                if userid == 'ALL':
                    sql = """ SELECT * FROM bounty_data WHERE `created_user_server`=%s 
                              ORDER BY `created_date` DESC LIMIT """+str(limit)+""" """
                    cur.execute(sql, (user_server))
                    result = cur.fetchall()
                    return result
                else:
                    sql = """ SELECT * FROM bounty_data WHERE `created_user_server`=%s AND `userid_create`=%s 
                              ORDER BY `created_date` DESC LIMIT """+str(limit)+""" """
                    cur.execute(sql, (user_server, userid))
                    result = cur.fetchall()
                    return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_bounty_get_ref(ref: str, created_user_server: str='DISCORD'):
    user_server = created_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT * FROM bounty_data WHERE `ref_id` = %s AND `created_user_server`=%s LIMIT 1 """
            cur.execute(sql, (ref, user_server))
            result = cur.fetchone()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_bounty_update_by_ref(ref: str, what: str, value: str, created_user_server: str='DISCORD'):
    user_server = created_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    if what not in ["bounty_title", "bounty_obj", "bounty_desc", "status", "amount"]:
        return
    if what.lower() == "amount":
        # TODO get amount string to list
        amount_data = json.loads(value)
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if what.lower() == "amount":
                sql = """ UPDATE bounty_data SET `coin_name` = %s, `bounty_amount`=%s, `bounty_amount_after_fee`=%s, 
                          `bounty_coin_decimal`=%s, `updated_date`=%s 
                          WHERE `ref_id` = %s AND `created_user_server`=%s """
                cur.execute(sql, (amount_data['coin_name'], amount_data['bounty_amount'], 
                                  amount_data['bounty_amount_after_fee'], amount_data['bounty_coin_decimal'], 
                                  int(time.time()), ref, user_server))
                conn.commit()
                return True
            else:
                sql = """ UPDATE bounty_data SET `""" + what.lower() + """` = %s, `updated_date`=%s 
                          WHERE `ref_id` = %s AND `created_user_server`=%s """
                cur.execute(sql, (value, int(time.time()), ref, user_server))
                conn.commit()
                return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_bounty_list_of_user_apply(userid: str, bounty_ref_id:str, created_user_server: str='DISCORD', status:str='APPLIED', limit: int=100):
    user_server = created_user_server.upper()
    status = status.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    bounty_ref_id = bounty_ref_id.upper()
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if userid != 'ALL' and bounty_ref_id == 'ALL':
                # select all applied by a user.
                sql = """ SELECT * FROM bounty_apply WHERE `applied_userid` = %s AND `created_user_server`=%s 
                          AND `status`=%s ORDER BY `applied_date` DESC LIMIT """+str(limit)+""" """
                cur.execute(sql, (userid, user_server, status))
                result = cur.fetchall()
                return result
            elif userid != 'ALL' and bounty_ref_id != 'ALL':
                # select an application by a user if exists
                sql = """ SELECT * FROM bounty_apply WHERE `applied_userid` = %s AND `created_user_server`=%s 
                          AND `status`=%s AND `bounty_ref_id`=%s LIMIT 1 """
                cur.execute(sql, (userid, user_server, status, bounty_ref_id))
                result = cur.fetchone()
                return result
            elif userid == 'ALL' and bounty_ref_id != 'ALL':
                # select all applicant for a bounty
                sql = """ SELECT * FROM bounty_apply WHERE `status`=%s 
                          AND `bounty_ref_id`=% ORDER BY `applied_date` DESC LIMIT """+str(limit)+""" """
                cur.execute(sql, (status, bounty_ref_id))
                result = cur.fetchall()
                return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_bounty_add_apply(coin_name: str, applied_id: str, applied_userid: str, bounty_ref_id: str, userid_create: str, bounty_amount: float, 
    bounty_amount_after_fee: float, bounty_coin_decimal: int, status: str, created_user_server: str='DISCORD'):
    global conn
    user_server = created_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    status = status.upper()
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ INSERT INTO `bounty_apply` (`applied_id`, `applied_userid`, `applied_date`, `bounty_ref_id`, 
                      `userid_create`, `bounty_amount`, `bounty_amount_after_fee`, `bounty_coin_decimal`, 
                      `status`, `created_user_server`, `coin_name`)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            cur.execute(sql, (applied_id, applied_userid, int(time.time()), bounty_ref_id, userid_create,
                              bounty_amount, bounty_amount_after_fee, bounty_coin_decimal, status, user_server, coin_name))
            conn.commit()
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False

def sql_bounty_list_of_apply(userid: str, status:str, created_user_server: str='DISCORD', limit: int=100):
    user_server = created_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    status = status.upper()
    if status not in ['ACCEPTED','REJECTED','APPLIED','COMPLETED','CANCELLED', 'ALL']:
        return
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if status != 'ALL':
                if userid == 'ALL':
                    sql = """ SELECT * FROM bounty_apply WHERE `status` = %s AND `created_user_server`=%s 
                              ORDER BY `applied_date` DESC LIMIT """+str(limit)+""" """
                    cur.execute(sql, (status, user_server))
                    result = cur.fetchall()
                    return result
                else:
                    sql = """ SELECT * FROM bounty_apply WHERE `status` = %s AND `created_user_server`=%s AND `applied_userid`=%s 
                              ORDER BY `applied_date` DESC LIMIT """+str(limit)+""" """
                    cur.execute(sql, (status, user_server, userid))
                    result = cur.fetchall()
                    return result
            else:
                if userid == 'ALL':
                    sql = """ SELECT * FROM bounty_apply WHERE `created_user_server`=%s 
                              ORDER BY `applied_date` DESC LIMIT """+str(limit)+""" """
                    cur.execute(sql, (user_server))
                    result = cur.fetchall()
                    return result
                else:
                    sql = """ SELECT * FROM bounty_apply WHERE `created_user_server`=%s AND `applied_userid`=%s 
                              ORDER BY `applied_date` DESC LIMIT """+str(limit)+""" """
                    cur.execute(sql, (user_server, userid))
                    result = cur.fetchall()
                    return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_bounty_get_apply_by_ref(userid: str, ref: str, created_user_server: str='DISCORD'):
    user_server = created_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if userid == 'ALL':
                sql = """ SELECT * FROM bounty_apply WHERE `bounty_ref_id` = %s  
                          ORDER BY `applied_date` DESC """
                cur.execute(sql, (ref))
                result = cur.fetchall()
                return result
            else:
                sql = """ SELECT * FROM bounty_apply WHERE `bounty_ref_id` = %s AND `created_user_server`=%s AND `applied_userid`=%s LIMIT 1 """
                cur.execute(sql, (ref, user_server, userid))
                result = cur.fetchone()
                return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_bounty_get_apply_by_app_ref(ref: str, app_ref: str, created_user_server: str='DISCORD'):
    user_server = created_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if app_ref == 'ALL':
                sql = """ SELECT * FROM bounty_apply WHERE `bounty_ref_id` = %s  
                          ORDER BY `applied_date` DESC """
                cur.execute(sql, (ref))
                result = cur.fetchall()
                return result
            else:
                sql = """ SELECT * FROM bounty_apply WHERE `applied_id` = %s AND `created_user_server`=%s LIMIT 1 """
                cur.execute(sql, (app_ref, user_server))
                result = cur.fetchone()
                return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_bounty_update_apply_by_ref(ref: str, userid: str, what: str, value: str, created_user_server: str='DISCORD'):
    user_server = created_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    if what not in [ "status"]:
        return
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ UPDATE bounty_apply SET `""" + what.lower() + """` = %s, `updated_date`=%s 
                      WHERE `bounty_ref_id` = %s AND `applied_userid`=%s AND `created_user_server`=%s """
            cur.execute(sql, (value, int(time.time()), ref, userid, user_server))
            conn.commit()
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None
