# core imports
from datetime import datetime, timedelta, date
from dateutil import parser
import json
import os
import time
import threading
import traceback
import signal
from decimal import Decimal
from io import StringIO
import logging

# external imports
import requests
import psycopg2
import numpy as np
import pandas as pd

# project imports
from utils import myencode, unpack_emitxfer_action, xfer_data_to_dict, save_dataframe, save_json, prepare_data_folder
from utils import TelegramNotifier

pd.options.display.max_columns = None
pd.options.display.float_format = '{:.8f}'.format

# load configuration settings
POSTGRES_USER = os.getenv('POSTGRES_USER', '')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', '')
POSTGRES_DB = os.getenv('POSTGRES_DB', '')
ACTION_COLLECTION_START_TIME = parser.parse(os.getenv('ACTION_COLLECTION_START_TIME', '2023-01-01'))
ACTION_COLLECTION_QUERY_INTERVAL_SECONDS = int(os.getenv('ACTION_COLLECTION_QUERY_INTERVAL_SECONDS', 30))
ACTION_COLLECTION_REPOPULATION_QUERY_INTERVAL_SECONDS = int(os.getenv('ACTION_COLLECTION_REPOPULATION_QUERY_INTERVAL_SECONDS', 5))
MATCHING_START_TIME = parser.parse(os.getenv('MATCHING_START_TIME', '2023-01-01'))
MATCHING_INTERVAL_SECONDS = int(os.getenv('MATCHING_INTERVAL_SECONDS', 5))
TELEGRAM_ALERT_BOT_KEY=os.getenv('TELEGRAM_ALERT_BOT_KEY', None)
TELEGRAM_ACCOUNTING_ALERT_CHAT_ID=int(os.getenv('TELEGRAM_ACCOUNTING_ALERT_CHAT_ID', 0))
TELEGRAM_TECHNICAL_ALERT_CHAT_ID=int(os.getenv('TELEGRAM_TECHNICAL_ALERT_CHAT_ID', 0))
LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'INFO').upper()

KEEP_RUNNING = True
def stop_container():
    global KEEP_RUNNING
    KEEP_RUNNING = False
signal.signal(signal.SIGTERM, stop_container)

class IndexerHealthMonitor:
    """
    A class for monitoring the health of indexers and storing the status in a database.
    """

    def __init__(self, clear_table: bool = False) -> None:
        """
        Initialize the class and establish a connection to the database.

        :param clear_table: If set to True, the indexer_health_status table will be dropped and recreated.
        """

        self.lock = threading.Lock()

        db_connection_ready = False
        while not db_connection_ready:
            try:
                self.pg_conn = psycopg2.connect(host="database", dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
                self.pg_cur = self.pg_conn.cursor()
                db_connection_ready = True
            except psycopg2.Error as e:
                logging.debug(f"Error connecting to the database: {e}. Retrying...")
                time.sleep(1)

        if clear_table:
            try:
                self.pg_cur.execute(f"""
                    DROP TABLE IF EXISTS indexer_health_status;
                    """)
                self.pg_conn.commit()
                logging.info('Dropped indexer_health_status table if present')
            except psycopg2.Error as e:
                logging.error(f"Error dropping indexer_health_status table: {e}")
                self.pg_conn.rollback()

        try:
            self.pg_cur.execute(f"""
                CREATE TABLE IF NOT EXISTS indexer_health_status (
                        chain varchar PRIMARY KEY,
                        time timestamp,
                        lib bigint,
                        status varchar,
                        note varchar,
                        error varchar
                    );
                """)
            self.pg_conn.commit()
            logging.info('Created indexer_health_status table if not present.')
        except psycopg2.Error as e:
            logging.error(f"Error creating indexer_health_status table: {e}")
            self.pg_conn.rollback()

    def update(self, chain: str = None, lib: int = None, status: str = None, note: str = '', error: str = '') -> None:
        """
        Update the indexer health status in the database.

        :param chain: The chain name.
        :param lib: The last irreversible block number.
        :param status: The status of the indexer (UP/DOWN).
        :param note: Additional notes about the indexer status.
        :param error: Error type if applicable.
        """

        with self.lock:
            try:
                self.pg_cur.execute("""
                    INSERT INTO indexer_health_status (chain, time, lib, status, note, error)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (chain)
                        DO UPDATE SET time=%s, lib=%s, status=%s, note=%s, error=%s;
                    """, (chain, datetime.utcnow(), lib, status, note, error, datetime.utcnow(), lib, status, note, error))
                self.pg_conn.commit()
            except psycopg2.Error as e:
                logging.error(f"Error upserting indexer_health_status record: {e}")
                self.pg_conn.rollback()

    def __del__(self):
        self.pg_conn.close()

class IndexerHealthException(Exception):
    """
    A simple exception subclass for handling errors relating to problems accessing the Hyperion API.
    """
    pass

class ActionCollectionThread(threading.Thread):
    """
    A class for collecting actions from a given API endpoint and storing the collected data in a PostgreSQL database.
    """

    def __init__(self, api_endpoint: str = None, clear_table: bool = False, chain: str = None, chain_info: dict = None,
                    start_time: datetime = datetime(2000, 1, 1), query_interval_seconds: int = 20, repopulate_query_interval_seconds: int = None,
                    indexer_health_monitor: object = None) -> None:
        """
        Initialize the instance of the class.

        :param api_endpoint: API endpoint to collect actions from.
        :param clear_table: Boolean flag to indicate if the database table should be cleared. Defaults to False.
        :param chain: Chain name.
        :param chain_info: Information about the chain from chains.json.
        :param start_time: Start time of the data collection. Defaults to `datetime(2000, 1, 1)`.
        :param query_interval_seconds: Interval between queries to the API endpoint. Defaults to 20.
        :param repopulate_query_interval_seconds: Interval between queries during the repopulation phase. Defaults to query_interval_seconds.
        :param indexer_health_monitor: An object that monitors the health of the indexer. Defaults to None.
        """

        super().__init__()
        self.api_endpoint = api_endpoint
        self.clear_table = clear_table
        self.chain = chain
        self.chain_info = chain_info
        self.actions_buffer = StringIO()
        self.row_limit = 500
        self.cursor_scroll_count = 1
        self.query_interval_seconds = query_interval_seconds
        self.repopulate_query_interval_seconds = repopulate_query_interval_seconds if repopulate_query_interval_seconds else query_interval_seconds
        self.repopulating = True
        self.base_table_name = f'actions_{chain}'
        accounts = [account for account in chain_info['wraplock_accounts'] + chain_info['wraptoken_accounts']]
        self.filter_param = ','.join([f'{a}:retire,{a}:emitxfer,{a}:issuea,{a}:issueb,{a}:withdrawa,{a}:withdrawb,{a}:cancela,{a}:cancelb' for a in accounts])
        # self.filter_param = ','.join([f'{a}:*' for a in accounts]) # use this if Hyperion can't handle long filters
        self.start_time = start_time
        self.lib = 0
        self.indexer_health_monitor = indexer_health_monitor

        # prepare database
        db_connection_ready = False
        while not db_connection_ready:
            try:
                self.pg_conn = psycopg2.connect(host="database", dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
                self.pg_cur = self.pg_conn.cursor()
                if self.clear_table:
                    self.dropDBBaseMetaTable()
                    self.dropDBTables()
                self.createDBBaseMetaTable()
                self.createDBTables()
                db_connection_ready = True
            except psycopg2.Error as e:
                logging.debug(f"Error connecting to the database: {e}. Retrying...")
                time.sleep(1)

    def run(self) -> None:
        """
        The main method that runs the data collection.
        """

        logging.info(f'Running ActionCollectionThread for chain: {self.chain}')
        while KEEP_RUNNING:
            try:
                try:
                    sql = f'select last_irreversible_action_time, last_irreversible_global_sequence_id from {self.base_table_name}_meta;'
                    self.pg_cur.execute(sql)
                    response = self.pg_cur.fetchone()
                    (self.last_irreversible_action_time, self.last_irreversible_global_sequence_id) = response
                    logging.debug(f'Last ireversible action time in {self.base_table_name}: {self.last_irreversible_action_time.strftime("%Y-%m-%dT%H:%M:%SZ")}')
                except:
                    logging.critical(traceback.format_exc())
                    break # exit app

                try:

                    logging.debug(f'Checking health of Hyperion {self.api_endpoint} ...')
                    response = requests.get(f'{self.api_endpoint}/v2/health', timeout=10)
                    response = response.json()

                    # handle errors from Hyperion API
                    if 'error' in response:
                        msg = f'Action data server {self.api_endpoint}: {response["message"]}'
                        raise IndexerHealthException(msg)

                    nodeos_head_time = parser.parse(response['health'][1]['service_data']['head_block_time'])
                    nodeos_head_block = int(response['health'][1]['service_data']['head_block_num'])
                    elasticsearch_last_indexed_block = int(response['health'][2]['service_data']['last_indexed_block'])
                    if (datetime.utcnow() - nodeos_head_time) > timedelta(seconds=15):
                        msg = f'Action data server {self.api_endpoint} head block is more than 15 seconds behind system time.'
                        raise IndexerHealthException(msg)
                    if (nodeos_head_block - elasticsearch_last_indexed_block) > 10:
                        msg = f'Action data server {self.api_endpoint} is more than 5 seconds out of sync.'
                        raise IndexerHealthException(msg)

                    logging.debug(f'Getting actions from Hyperion {self.api_endpoint} ...')
                    actions = []
                    for qi in range(self.cursor_scroll_count):
                        url = f'{self.api_endpoint}/v2/history/get_actions'
                        params = {
                            'sort': 'asc',
                            'after': self.last_irreversible_action_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                            'skip': qi * self.row_limit,
                            'limit': self.row_limit,
                            'checkLib': 'true'
                        }
                        if self.filter_param:
                            params['filter'] = self.filter_param
                        response = requests.get(url, params=params, timeout=20)
                        response = response.json()

                        # handle errors from Hyperion API
                        if 'error' in response:
                            msg = f'Action data server {self.api_endpoint}: {response["message"]}'
                            raise IndexerHealthException(msg)

                        self.lib = response['lib']
                        actions += response['actions']

                        if len(actions) < self.row_limit - 20: # don't do more queries if few rows
                            break

                except IndexerHealthException as e:
                    self.indexer_health_monitor.update(chain=self.chain, lib=self.lib, status='DOWN', note=str(e), error='Indexer Health Error')
                    logging.info(e)
                    time.sleep(3)
                    continue

                except requests.exceptions.RequestException as e:
                    msg = f'Action data server {self.api_endpoint} cannot be reached.'
                    self.indexer_health_monitor.update(chain=self.chain, lib=self.lib, status='DOWN', note=msg, error='Request Error')
                    logging.info(msg)
                    time.sleep(3)
                    continue

                except Exception as e:
                    msg = f'Action data indexing problem for chain {self.chain}, check logs!'
                    self.indexer_health_monitor.update(chain=self.chain, lib=self.lib, status='DOWN', note=msg, error='Unspecified Error')
                    logging.info(msg)
                    logging.debug(traceback.format_exc())
                    time.sleep(3)
                    continue

                self.new_irreversible_global_sequence_id = self.last_irreversible_global_sequence_id
                self.new_irreversible_action_time = self.last_irreversible_action_time
                try:
                    action_ids = []
                    max_action_count = self.row_limit * self.cursor_scroll_count
                    actions_added = 0
                    self.current_action_group = []
                    last_trx_id = ''
                    for action_id, action in enumerate(actions):
                        global_sequence_id = action['global_sequence']
                        trx_id = action['trx_id']

                        # check ensure action group boundaries are respected so inline grouping can be done if required
                        if trx_id != last_trx_id: # 
                            if (action_id <= (max_action_count - 20)): # and not near end of action list
                                if global_sequence_id not in action_ids: # and action not duplicated
                                    if len(self.current_action_group) > 0:
                                        self.processActionsToBuffers()

                                    # start new action group and add action
                                    self.current_action_group = [action]
                            else:
                                # exit loop
                                break
                        else: # if is child action
                            if global_sequence_id not in action_ids: # and action not duplicated
                                # add action to action group
                                self.current_action_group.append(action)

                        last_trx_id = trx_id
                        action_ids.append(global_sequence_id) # for duplicate check

                    if len(self.current_action_group) > 0:
                        self.processActionsToBuffers()

                    try:
                        self.updateFromActionsBuffers()
                        self.pg_cur.execute(f"""UPDATE {self.base_table_name}_meta
                                                SET last_irreversible_action_time = '{self.new_irreversible_action_time.strftime('%Y-%m-%d %H:%M:%S')}',
                                                    last_irreversible_global_sequence_id = {int(self.new_irreversible_global_sequence_id)},
                                                    lib = {int(self.lib)};
                                                """)
                        self.pg_conn.commit()
                        self.actions_buffer = StringIO()
                        logging.debug(f'Added/replaced actions from Hyperion.')

                        # to reduce the frequency of updates once collected all the current data
                        if self.repopulating:
                            if self.new_irreversible_global_sequence_id == self.last_irreversible_global_sequence_id:
                                self.repopulating = False
                                logging.info(f'Finished collecting historic data for {self.chain}.')
                                self.indexer_health_monitor.update(chain=self.chain, lib=self.lib, status='UP')
                            else:
                                self.indexer_health_monitor.update(chain=self.chain, lib=self.lib, status='DOWN', note='Currently collecting historic data', error='')
                        else:
                            self.indexer_health_monitor.update(chain=self.chain, lib=self.lib, status='UP')

                    except Exception as e:
                        self.pg_conn.rollback()
                        logging.error(f'Could not commit new Hyperion actions: {e}')

                except Exception as e:
                    logging.critical(f'{traceback.format_exc()}')
                    break # exit app

            except Exception as e:
                logging.critical(f'{traceback.format_exc()}')
                break # exit app

            if self.repopulating:
                time.sleep(self.repopulate_query_interval_seconds)
            else:
                time.sleep(self.query_interval_seconds)

    def dropDBBaseMetaTable(self) -> None:
        """
        Drops the metadata table in the PostgreSQL database if it exists.
        """

        try:
            self.pg_cur.execute(f"""
                DROP TABLE IF EXISTS {self.base_table_name}_meta;
                """)
            self.pg_conn.commit()
            logging.info('Dropped meta table if present')
        except psycopg2.Error as e:
            logging.error(f"Error dropping meta table: {e}")
            self.pg_conn.rollback()

    def createDBBaseMetaTable(self) -> None:
        """
        Creates the metadata table in the PostgreSQL database if it does not exist.
        """

        try:
            self.pg_cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.base_table_name}_meta (
                        last_irreversible_action_time timestamp,
                        last_irreversible_global_sequence_id bigint,
                        lib bigint
                    );
                INSERT INTO {self.base_table_name}_meta (last_irreversible_action_time, last_irreversible_global_sequence_id, lib) 
                    VALUES ('{self.start_time.strftime('%Y-%m-%d %H:%M:%S')}', 0, 0);
                """)
            self.pg_conn.commit()
            logging.info('Created meta table if not present.')
        except psycopg2.Error as e:
            logging.error(f"Error creating meta table: {e}")
            self.pg_conn.rollback()

    def dropDBTables(self) -> None:
        """
        Drops the data table in the PostgreSQL database if it exists.
        """

        try:
            self.pg_cur.execute(f"""
                DROP TABLE IF EXISTS {self.base_table_name};
                """)
            self.pg_conn.commit()
            logging.info('Dropped data tables if present.')
        except psycopg2.Error as e:
            logging.error(f"Error dropping data table: {e}")
            self.pg_conn.rollback()

    def createDBTables(self) -> None:
        """
        Creates the data table in the PostgreSQL database if it does not exist.
        """

        try:
            self.pg_cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.base_table_name} (
                        global_sequence_id bigint PRIMARY KEY,
                        tx varchar,
                        time timestamp,
                        source_chain varchar,
                        destination_chain varchar,
                        action varchar,
                        owner varchar,
                        beneficiary varchar,
                        contract varchar,
                        symbol varchar,
                        quantity numeric(20, 8),
                        xfer_global_sequence_id bigint,
                        excluded_matched boolean DEFAULT FALSE
                    );
                CREATE INDEX IF NOT EXISTS {self.base_table_name}_idx_1 ON {self.base_table_name} (xfer_global_sequence_id, source_chain, destination_chain);
                CREATE INDEX IF NOT EXISTS {self.base_table_name}_idx_2 ON {self.base_table_name} (excluded_matched, xfer_global_sequence_id, source_chain, destination_chain);
                """)
            self.pg_conn.commit()
            logging.info('Created data tables if not present.')
        except psycopg2.Error as e:
            logging.error(f"Error creating data table: {e}")
            self.pg_conn.rollback()

    def processActionsToBuffers(self) -> None:
        """
        Process the current action group, and write the processed actions to the actions buffer.
        """

        def write_to_buffer(group_last_global_sequence_id, txid, timestamp, source_chain, destination_chain, action, owner, beneficiary, contract, symbol, quantity, xfer_global_sequence_id):
            """
            Write the action data to the actions buffer.
            """

            if group_last_global_sequence_id > self.last_irreversible_global_sequence_id:
                line = f'{group_last_global_sequence_id},{txid},{timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")},{source_chain},{destination_chain},{action},{owner},{beneficiary},{contract},{symbol},{quantity},{xfer_global_sequence_id}\n'
                self.actions_buffer.write(line) # write as csv

        actions = self.current_action_group
        try:
            block_no = actions[0]['block_num']
            timestamp = datetime.strptime(actions[0]['@timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
            group_last_global_sequence_id = 0

            for action_index, action in enumerate(self.current_action_group):

                account = atype = source_chain = destination_chain = contract = symbol = ''
                xfer = {}

                txid = action['trx_id']
                act = action['act']
                account = act['account']
                name = act['name']
                group_last_global_sequence_id = action['global_sequence']
                data = act['data']

                if name in ['emitxfer','issuea','issueb','withdrawa','withdrawb','cancela','cancelb']:

                    if name == 'emitxfer':
                        xfer = xfer_data_to_dict(data)

                        if len(self.current_action_group) == 1: # must have been triggered by transfer
                            atype = 'lock'
                        else:
                            atype = self.current_action_group[action_index-1]['act']['name'] # get parent action name (retire/cancel)
                            if atype in ['cancela', 'cancelb']:
                                atype = 'cancel' # for easier aggregation

                        xfer_global_sequence_id = action['global_sequence']
                        source_chain = self.chain
                        destination_chain = account.split('.')[2]

                    else:
                        atype = name
                        source_chain = account.split('.')[2]
                        destination_chain = self.chain

                        # get original action from proof
                        hex_data = data['actionproof']['action']['data']
                        xfer_global_sequence_id = data['actionproof']['receipt']['global_sequence']
                        xfer = xfer_data_to_dict(unpack_emitxfer_action(hex_data))

                    write_to_buffer(group_last_global_sequence_id, txid, timestamp, source_chain, destination_chain, atype, xfer.get('owner',''), xfer.get('beneficiary',''), xfer.get('contract',''), xfer.get('symbol', ''), xfer.get('quantity',''), xfer_global_sequence_id)

            if block_no <= self.lib:
                self.new_irreversible_global_sequence_id = group_last_global_sequence_id
                self.new_irreversible_action_time = timestamp

        except Exception as e:
            logging.error(f"Error processing actions!")
            logging.debug(traceback.format_exc())


    def updateFromActionsBuffers(self) -> None:
        """
        This function updates the PostgreSQL database table with the contents of the actions buffer.
        """

        self.pg_cur.execute(f'DELETE FROM {self.base_table_name} WHERE global_sequence_id > {self.last_irreversible_global_sequence_id}')
        self.actions_buffer.seek(0)
        self.pg_cur.copy_from(self.actions_buffer, f'{self.base_table_name}', sep=f',',
                                columns=('global_sequence_id', 'tx', 'time', 'source_chain', 'destination_chain', 'action', 'owner', 'beneficiary', 'contract', 'symbol', 'quantity', 'xfer_global_sequence_id'))

    def __del__(self) -> None:
        self.pg_conn.close()


class MatchingThread(threading.Thread):
    """
    The MatchingThread class performs the matching of transfers between chains. It is a subclass of the `threading.Thread` class.
    """

    def __init__(self, chains: dict, start_time: datetime = datetime(2000, 1, 1), interval_seconds: int = 20,
                    indexer_health_monitor: object = None) -> None:
        """
        The constructor for the MatchingThread class.

        :param chains: A dictionary of chains.
        :param start_time: The start time for the matching. Defaults to `datetime(2000, 1, 1)`.
        :param interval_seconds: The interval in seconds between each matching. Defaults to 20.
        :param indexer_health_monitor: An object that monitors the health of the indexer. Defaults to None.
        """

        super().__init__()
        self.chains = chains
        self.start_time = start_time
        self.pg_conn = psycopg2.connect(host="database", dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
        self.pg_cur = self.pg_conn.cursor()
        self.interval_seconds = interval_seconds
        self.indexer_health_monitor = indexer_health_monitor

    def match_to_df(self) -> pd.DataFrame:
        """
        The method the carries out the main cross chain matching query and returns a dataframe.

        :return: The pandas dataframe corresponding to the database table join.

        """
        dfs = []
        for source_chain in self.chains.keys():
            for destination_chain in self.chains.keys():
                if source_chain != destination_chain:
                    query = f"""
                        SELECT  s.source_chain,
                                s.action source_action,
                                s.time source_time,
                                s.destination_chain,
                                d.action destination_action,
                                d.time destination_time,
                                s.owner,
                                s.beneficiary,
                                s.quantity,
                                s.contract,
                                s.symbol,
                                d.owner d_owner,
                                d.beneficiary d_beneficiary,
                                d.quantity d_quantity,
                                d.contract d_contract,
                                d.symbol d_symbol,
                                s.tx source_tx,
                                d.tx destination_tx,
                                s.global_sequence_id source_gid,
                                d.global_sequence_id destination_gid,
                                CASE
                                    WHEN s.owner = d.owner
                                    AND s.beneficiary = d.beneficiary
                                    AND s.quantity = d.quantity
                                    AND s.contract = d.contract
                                    AND s.symbol = d.symbol
                                    THEN true ELSE false END AS "xfer_match"
                        FROM actions_{source_chain} s INNER JOIN actions_{destination_chain} d
                        ON s.xfer_global_sequence_id = d.xfer_global_sequence_id
                        WHERE s.source_chain = d.source_chain AND s.destination_chain = d.destination_chain
                        AND (s.action = 'lock' OR s.action = 'retire')
                        AND (s.excluded_matched = false AND d.excluded_matched = false)
                        ORDER BY s.time;
                    """

                    df = pd.read_sql(query, self.pg_conn, parse_dates=['time_s', 'time_d'])
                    dfs.append(df)

        df_matched = pd.concat(dfs)
        df_matched = df_matched.sort_values(by='source_time', ascending=True).reset_index(drop=True)

        return df_matched

    def check_mark_old_database_records_as_matched(self, df_matched: pd.DataFrame) -> bool:
        """
        The method updates matching table records prior to the matching start_time, marking them as matched.

        :param df_matched: The pandas dataframe corresponding to the database table join.
        :return: True if any records were marked as matched.

        """
        df_matched_old = df_matched[df_matched['source_time'] < self.start_time]

        matches_marked = False

        source_gid_list = df_matched_old.groupby('source_chain')['source_gid'].apply(list).to_dict()
        for chain, gids in source_gid_list.items():
            logging.debug(f'Marking the following database records as matched for {chain}')
            logging.debug(gids)
            if len(gids) > 0:
                global_sequence_ids = ",".join(str(x) for x in gids)
                self.pg_cur.execute(f"""
                    UPDATE actions_{chain} SET excluded_matched = true
                        WHERE global_sequence_id IN  ({global_sequence_ids});
                    """)
                self.pg_conn.commit()
                matches_marked = True

        destination_gid_list = df_matched_old.groupby('destination_chain')['destination_gid'].apply(list).to_dict()
        for chain, gids in destination_gid_list.items():
            logging.debug(f'Marking the following database records as matched for {chain}')
            logging.debug(gids)
            if len(gids) > 0:
                global_sequence_ids = ",".join(str(x) for x in gids)
                self.pg_cur.execute(f"""
                    UPDATE actions_{chain} SET excluded_matched = true
                        WHERE global_sequence_id IN  ({global_sequence_ids});
                    """)
                self.pg_conn.commit()
                matches_marked = True

        return matches_marked

    def run(self) -> None:
        """
        The method that runs the matching process. It performs the matching every `interval_seconds` seconds,
        The matching process consists of the following steps:
            1. Check the status of the source data API servers.
            2. Match the transfers between chains.
            3. Save the successful transfers, transfers with discrepancies, and unmatched proofs.
            4. If there are any unmatched proofs on chain pairs for which both health statuses are 'UP', issue notifications and save warning data.
        """

        time.sleep(5)
        logging.info('Running %s' % type(self).__name__)

        accounting_alert = TelegramNotifier(
            bot_key=TELEGRAM_ALERT_BOT_KEY,
            chat_id=TELEGRAM_ACCOUNTING_ALERT_CHAT_ID,
            logging=logging,
            duplicates_suppressed_seconds=30
            )

        technical_alert = TelegramNotifier(
            bot_key=TELEGRAM_ALERT_BOT_KEY,
            chat_id=TELEGRAM_TECHNICAL_ALERT_CHAT_ID,
            logging=logging,
            duplicates_suppressed_seconds=30
            )


        while KEEP_RUNNING:

            start_time = time.time()

            # check status of source data API servers
            indexer_health_statuses = {x: {'status': 'DOWN', 'reason': 'No database entry found.', 'time': datetime.utcnow().isoformat()} for x in self.chains.keys()}
            df = pd.read_sql(f"SELECT * FROM indexer_health_status", self.pg_conn, parse_dates=['time'])
            for index, row in df.iterrows():
                api_endpoint = self.chains[row['chain']]['api_endpoint']
                if row['time'] < datetime.utcnow() - timedelta(minutes=2):
                    indexer_health_statuses[row['chain']] = {'status': 'DOWN', 'time': row['time'].to_pydatetime().isoformat(), 'reason': f'Data from {api_endpoint} has not been received in the last 2 minutes.'}
                    technical_alert.send(f'IBC Monitor: Data from {api_endpoint} has not been received in the last 2 minutes.')
                else:
                    if row['status'] != 'UP':
                        indexer_health_statuses[row['chain']] = {'status': 'DOWN', 'time': row['time'].to_pydatetime().isoformat(), 'reason': row['note']}
                        technical_alert.send(f"IBC Monitor: {row['note']}")
                    else:
                        indexer_health_statuses[row['chain']] = {'status': 'UP', 'time': row['time'].to_pydatetime().isoformat()}

            save_json('indexer_health_status', indexer_health_statuses)
            logging.debug('Saved indexer_health_statuses json')

            # MATCHED TRANSFERS
            df_matched = self.match_to_df()

            if self.check_mark_old_database_records_as_matched(df_matched):
                df_matched = self.match_to_df() # if new match marks were made, fetch dataframe again

            df_matched_with_discrepancies = df_matched[df_matched['xfer_match'] == False]
            if len(df_matched_with_discrepancies.index) > 0:
                df_matched = df_matched[df_matched['xfer_match'] == True]
            save_dataframe('successful_transfers', df_matched)
            logging.debug('Saved successful_transfers dataframe')

            save_dataframe('successful_transfers_with_discrepancies', df_matched_with_discrepancies)
            logging.debug('Saved successful_transfers_with_discrepancies dataframe')

            source_xfer_gids = df_matched['source_gid'].unique().tolist() + df_matched_with_discrepancies['source_gid'].unique().tolist()

            # FAILURES
            dfs = []
            for chain in self.chains.keys():
                query = f"""
                    SELECT  source_chain,
                            action source_action,
                            time,
                            destination_chain,
                            owner,
                            beneficiary,
                            quantity,
                            contract,
                            symbol,
                            tx,
                            global_sequence_id
                    FROM actions_{chain} a
                    WHERE time >= '{self.start_time}'
                    AND (NOT a.xfer_global_sequence_id = ANY(%s))
                    AND (a.action = 'lock' OR a.action = 'retire')
                    AND excluded_matched = false
                    ORDER BY a.time;
                """       

                df = pd.read_sql(query, self.pg_conn, params=(source_xfer_gids,), parse_dates=['time'])
                dfs.append(df)

            df_outstanding = pd.concat(dfs).reset_index(drop=True)
            df_outstanding = df_outstanding.sort_values(by='time', ascending=True).reset_index(drop=True)
            save_dataframe('outstanding_transfers', df_outstanding)
            logging.debug('Saved outstanding_transfers dataframe')

            # UNMATCHED PROOFS (none should exist if all actions were collected!)
            dfs = []
            for chain in self.chains.keys():
                query = f"""
                    SELECT  source_chain,
                            destination_chain,
                            action destination_action,
                            time,
                            owner,
                            beneficiary,
                            quantity,
                            contract,
                            symbol,
                            tx,
                            global_sequence_id
                    FROM actions_{chain} a
                    WHERE time >= '{self.start_time}'
                    AND (NOT a.xfer_global_sequence_id = ANY(%s))
                    AND (a.action = 'withdrawa' OR a.action = 'issuea' OR a.action = 'cancela'
                      OR a.action = 'withdrawb' OR a.action = 'issueb' OR a.action = 'cancelb')
                    AND excluded_matched = false
                    ORDER BY a.time;
                """

                df = pd.read_sql(query, self.pg_conn, params=(source_xfer_gids,), parse_dates=['time'])
                dfs.append(df)

            df_unmatched = pd.concat(dfs)
            df_unmatched = df_unmatched.sort_values(by='time', ascending=True).reset_index(drop=True)
            save_dataframe('unmatched_proofs', df_unmatched)
            logging.debug('Saved unmatched_proofs dataframe')

            end_time = time.time()
            logging.debug(f'Match took {end_time - start_time} seconds')

            # send telegram alerts for proofs with no corresponding lock action
            for index, row in df_unmatched.iterrows():
                # as long as source API is up, destination should never have unmatched proofs
                if indexer_health_statuses[row['source_chain']]['status'] == 'UP':
                    destination_link_prefix = self.chains[row['destination_chain']]['block_explorer_tx_link_prefix']
                    message = f"Unmatched Proof Detected!\n"
                    message += f"[Transaction on {row['destination_chain'].upper()}]({destination_link_prefix}{row['tx']})\n"
                    logging.critical(message)
                    accounting_alert.send(message)

            # send telegram alerts for successful transfers with discrepancies
            for index, row in df_matched_with_discrepancies.iterrows():
                source_link_prefix = self.chains[row['source_chain']]['block_explorer_tx_link_prefix']
                destination_link_prefix = self.chains[row['destination_chain']]['block_explorer_tx_link_prefix']
                message = f"Transfer with Discrepancies Detected!\n"
                message += f"[Transaction on {row['source_chain'].upper()}]({source_link_prefix}{row['source_tx']})\n"
                message += f"[Transaction on {row['destination_chain'].upper()}]({destination_link_prefix}{row['destination_tx']})\n"
                logging.critical(message)
                accounting_alert.send(message)

            time.sleep(self.interval_seconds)

    def __del__(self) -> None:
        self.pg_conn.close()

if __name__ == '__main__':

    # logging configuration
    logging.basicConfig(
        level=LOGGING_LEVEL,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # sanity check
    if MATCHING_START_TIME < ACTION_COLLECTION_START_TIME:
        logging.error('MATCHING_START_TIME must not be earlier than ACTION_COLLECTION_START_TIME in config.env')
        time.sleep(1)
        quit()

    # get chain actions data requirements
    with open('chains.json', 'r') as chains_file:
        chains = json.load(chains_file)

    prepare_data_folder()

    # create shared instance of indexer health monitor object
    indexer_health_monitor = IndexerHealthMonitor(clear_table=False)

    # run action collection thread for each chain
    for chain, info in chains.items():
        ActionCollectionThread(api_endpoint=info['api_endpoint'], chain=chain, chain_info=info, start_time=ACTION_COLLECTION_START_TIME,
                                clear_table=False, query_interval_seconds=ACTION_COLLECTION_QUERY_INTERVAL_SECONDS,
                                repopulate_query_interval_seconds=ACTION_COLLECTION_REPOPULATION_QUERY_INTERVAL_SECONDS,
                                indexer_health_monitor=indexer_health_monitor).start()

    # run matching thread
    MatchingThread(chains=chains, start_time=MATCHING_START_TIME, interval_seconds=MATCHING_INTERVAL_SECONDS,
                    indexer_health_monitor=indexer_health_monitor).start()