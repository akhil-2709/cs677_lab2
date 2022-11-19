import threading
from tempfile import NamedTemporaryFile
import shutil
from typing import Dict

import csv
import json

# Log a transaction
from logger import get_logger
from peer.model import Peer
data_lock = threading.Lock()

LOGGER = get_logger(__name__)


# def log_transaction(filename, log):
#     log = json.dumps(log)
#     with open(filename, 'a') as csvF:
#         csvWriter = csv_files.writer(csvF, delimiter=' ')
#         csvWriter.writerow([log])
#
#
# # Mark Transaction Complete
# def mark_transaction_complete(filename, transaction, identifier):
#     tempfile = NamedTemporaryFile(delete=False)
#     with open(filename, 'rb') as csvFile, tempfile:
#         reader = csv_files.reader(csvFile, delimiter=' ')
#         writer = csv_files.writer(tempfile, delimiter=' ')
#         for row in reader:
#             row = json.loads(row[0])
#             k, _ = row.items()[0]
#             if k == identifier:
#                 row[k]['completed'] = True
#             row = json.dumps(row)
#             writer.writerow([row])
#     shutil.move(tempfile.name, filename)
#
#
# # Log the seller info
# def seller_log(trade_list):
#     with open('seller_info.csv_files', 'wb') as csvF:
#         csvWriter = csv_files.writer(csvF, delimiter=' ')
#         for k, v in trade_list.iteritems():
#             log = json.dumps({k: v})
#             csvWriter.writerow([log])
#
#         # Read the seller log
#
#
# def read_seller_log():
#     with open('seller_info.csv_files', 'rb') as csvF:
#         seller_log = csv_files.reader(csvF, delimiter=' ')
#         dictionary = {}
#         for log in seller_log:
#             log = json.loads(log[0])
#             k, v = log.items()[0]
#             dictionary[k] = v
#     return dictionary
#
#
# # Return any unserved requests.
# def get_unserved_requests():
#     with open('transactions.csv_files', 'rb') as csvF:
#         transaction_log = csv_files.reader(csvF, delimiter=' ')
#         open_requests = []
#         transaction_list = list(transaction_log)
#         last_request = json.loads(transaction_list[len(transaction_list) - 1][0])
#         _, v = last_request.items()[0]
#         if v['completed'] == False:
#             return last_request
#         else:
#             return None


def write_peers(network: Dict[str, Peer]):
    with data_lock:
        LOGGER.info("Writing into a file")
        with open('data.json', 'w') as f:
            json.dump(network, f)

def get_peers():
    with data_lock:
        LOGGER.info("Reading into a file")
        with open('peers.json', 'r') as file:
            data = json.load(file)
        return data


