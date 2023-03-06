# core imports
import os
import glob
import hashlib
import pickle
from datetime import datetime, timedelta
from collections import deque

# external imports
import requests
import pandas as pd
import simplejson as json
from decimal import Decimal
from ueosio import DataStream

def prepare_data_folder() -> None:
    """
    Creates a folder named 'dataframes' if it doesn't already exist, else does nothing.
    Deletes all files in the folder 'dataframes'.
    """

    try: os.mkdir('dataframes')
    except: pass

    for file in glob.glob(os.path.join('dataframes', '*')):
        try: os.remove(file)
        except: pass

def xfer_data_to_dict(data: dict) -> dict:
    """
    Transforms the IBC transfer data into a dictionary.

    :param data: The dict representing the data from the emitxfer action.
    :return: A transformed dict containing the owner, quantity, contract, symbol and beneficiary.
    """

    return {
        'owner': data['xfer']['owner'],
        'quantity': Decimal(data['xfer']['quantity']['quantity'].split(' ')[0]),
        'contract': data['xfer']['quantity']['contract'],
        'symbol': data['xfer']['quantity']['quantity'].split(' ')[1],
        'beneficiary': data['xfer']['beneficiary']
        }


def unpack_emitxfer_action(hex_data: str, as_dict: bool = False) -> dict:
    """
    Unpacks the given hex data for a raw emitxfer action.

    :param hex_data (str): The hex data from the emitxfer action to be unpacked.
    :param as_dict (bool): If true, returns the unpacked data as a flat dictionary, else as a nested dictionary representing the original action.
    :return: A dictionary containing the unpacked data.
    """

    data = bytes.fromhex(hex_data)
    ds = DataStream(data)
    owner = ds.unpack_name()
    quantity = dict(ds.unpack_extended_asset())
    beneficiary = ds.unpack_name()
    q_split = quantity['quantity'].split(',')
    if as_dict:
        return {
            'owner': owner,
            'quantity': Decimal(q_split[0]) / Decimal(10**int(q_split[1])),
            'contract': quantity['contract'],
            'symbol': q_split[2],
            'beneficiary': beneficiary
            }
    else:
        return {
            'xfer': {
                "beneficiary": beneficiary,
                "owner": owner,
                "quantity": {
                    "contract": quantity['contract'],
                    "quantity": "{:.{}f}".format(Decimal(q_split[0]) / Decimal(10**int(q_split[1])), int(q_split[1])) + ' ' + q_split[2]
                }
            }
        }

def pack_emitxfer_action(action: dict) -> str:
    """
    Packs the given emitxfer action into hex data.

    :param action: The emitxfer action to be packed.
    :return: The hex data for the packed action.
    """

    ds = DataStream()
    ds.pack_name(action['xfer']['owner'])
    ds.pack_asset(action['xfer']['quantity']['quantity'])
    ds.pack_name(action['xfer']['quantity']['contract'])
    ds.pack_name(action['xfer']['beneficiary'])
    return ds.getvalue().hex()

def test_pack_unpack_emitxfer_action() -> None:
    """
    Tests that the emitxfer action can be successfully packed and unpacked into hex data.
    """

    x = "a09861f950968667105e5f0000000000045554580000000000a6823403ea3055104a08013baca662"
    y = unpack_emitxfer_action(x)
    z = pack_emitxfer_action(y)
    assert x == z


def myencode(data: object) -> bytes:
    """
    Encodes to JSON formatted string.

    :param data: A python object to be encoded to JSON format.
    :return: A byte string representation of the JSON encoded data.
    """

    return json.dumps(data).encode("utf-8")

def save_dataframe(filename: str, df: pd.DataFrame) -> None:
    """
    This function takes a pandas DataFrame and saves it to a file with the given filename.

    :param filename: The name of the file to save the pandas DataFrame.
    :param df: The pandas DataFrame to save.
    """

    with open(f"dataframes/{filename}.pkl", "wb") as f:
        pickle.dump(df, f)

def load_dataframe(filename: str) -> pd.DataFrame:
    """
    This function takes a filename of a file that contains a saved pandas DataFrame and returns the DataFrame.

    :param filename: The name of the file containing the saved pandas DataFrame.
    :return: The pandas DataFrame loaded from the file.
    """

    with open(f"dataframes/{filename}.pkl", "rb") as f:
        return pickle.load(f)

def save_json(filename: str, js: object) -> None:
    """
    This function takes a JSON object and saves it to a file with the given filename.

    :param filename: The name of the file to save the JSON data.
    :param js: The JSON data to save.
    """

    with open(f"dataframes/{filename}.json", "w") as f:
        json.dump(js, f)

def load_json(filename: str) -> object:
    """
    This function takes a filename of a file that contains a JSON object and returns the object.

    :param filename: The name of the file containing the JSON data.
    :return: The JSON data loaded from the file.
    """

    with open(f"dataframes/{filename}.json", "r") as f:
        return json.load(f)

class TelegramNotifier:
    """
    This class represents a Telegram notifier that sends messages to a specified chat using a Telegram bot.

    :param bot_key: The API key of the Telegram bot.
    :param chat_id: The chat ID of the Telegram chat.
    :param duplicates_suppressed_seconds: The number of seconds to suppress duplicate messages.
    :param logging: An instance of the logging module for error reporting.
    """

    def __init__(self, bot_key: str, chat_id: int, logging: object, duplicates_suppressed_seconds:int = 10) -> None:
        self.bot_key = bot_key
        self.chat_id = chat_id
        self.duplicates_suppressed_seconds = duplicates_suppressed_seconds
        self.logging = logging
        self.recent_messages = deque(maxlen=10)

    def check_add_message_to_recent_list(self, markdown: str) -> bool:
        """
        This method checks if a message is a duplicate and if not, adds it to the recent messages list.

        :param markdown: The message to check and add to the list.
        :return: True if the message was added to the list, False otherwise.
        """
        for tm, message in self.recent_messages:
            if tm > datetime.utcnow() - timedelta(seconds=self.duplicates_suppressed_seconds):
                if message == markdown:
                    return False
        self.recent_messages.append((datetime.utcnow(), markdown))
        return True

    def send(self, markdown: str) -> bool:
        """
        This method sends a message to the Telegram chat using the Telegram bot.

        :param markdown: The message to send.
        :return: True if the message was sent successfully (or supressed as duplicate), False otherwise.
        """
        try:
            if self.check_add_message_to_recent_list(markdown):
                base_url = f'https://api.telegram.org/bot{self.bot_key}/sendMessage'
                data = {'chat_id': self.chat_id, 'parse_mode': 'markdown', 'disable_web_page_preview': 'true', 'text': markdown}
                requests.post(base_url, data=data, timeout=10).json()
                self.logging.info(f'Sent Telegram message: {markdown}')
            return True
        except Exception as e:
            self.logging.error(f'Could not send Telegram message: {e}')
            return False
