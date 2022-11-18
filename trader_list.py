import threading
from typing import Dict

from logger import get_logger
from peer.model import Peer

LOGGER = get_logger(__name__)


class TraderList:
    def __init__(self,
                 network: Dict[str, Peer]):
        self._network = network
        self._trader_list = []
        self._trader_list_lock = threading.Lock()

    def get_trader_list(self):
        return self._trader_list

    def set_trader_list(self, seller_id):
        LOGGER.info(f"seller in set_trader_list : {seller_id}")
        with self._trader_list_lock:
            seller_obj = self._network[seller_id]
            LOGGER.info(f"seller : {seller_id}, product: {seller_obj.item},product_count:{seller_obj.quantity}")
            self._trader_list.append((seller_obj, seller_obj.lamport))


