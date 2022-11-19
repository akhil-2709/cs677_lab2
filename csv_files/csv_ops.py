import threading
from tempfile import NamedTemporaryFile
import shutil
from typing import Dict, List

import csv
import json

# Log a transaction
from logger import get_logger
from peer.model import Peer
seller_lock = threading.Lock()
peers_lock = threading.Lock()

LOGGER = get_logger(__name__)

def write_sellers(seller_list :List):
    with seller_lock:
        LOGGER.info("Writing sellers into a file")
        with open('sellers.txt', 'w',encoding='utf-8', errors='ignore') as f:
            for item in seller_list:
                # write each item on a new line
                f.write("%s\n" % item)
            LOGGER.info("Writing to a file is successful")

def get_sellers():
    seller_list = []
    with seller_lock:
        LOGGER.info("Reading sellers from a file")
        with open('sellers.txt', 'r',encoding='utf-8', errors='ignore') as f:
            LOGGER.info("opened sellers.txt file")
            if f:
                for line in f:
                    x = line[:-1]
                    if x != '':
                        seller_list.append(x)
            LOGGER.info(f"sellers list; {seller_list}")
        LOGGER.info("Reading from a sellers file is successful")
        return seller_list


def write_peers(network: Dict[str, Peer]):
    with peers_lock:
        LOGGER.info("Writing into a file")
        with open('peers.json', 'w',encoding='utf-8', errors='ignore') as f:
            json.dump(network, f)
        LOGGER.info("Writing to a file is successful")

def get_peers():
    with peers_lock:
        LOGGER.info("Reading from a file")
        with open('peers.json', 'r',encoding='utf-8', errors='ignore') as file:
            data = json.load(file, strict=False)
        LOGGER.info("Reading from a file is successful")
        return data


