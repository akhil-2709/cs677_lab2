import threading
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from typing import List, Dict

import Pyro4

from enums.item_type import Item
from logger import get_logger
from peer.model import Peer
from rpc.rpc_helper import RpcHelper

LOGGER = get_logger(__name__)


@Pyro4.expose
class CommonOps(ABC):
    def __init__(self,
                 network: Dict[str, Peer],
                 current_peer: Peer,
                 thread_pool_size=20):
        self._current_peer = current_peer
        self._pool = ThreadPoolExecutor(max_workers=thread_pool_size)
        self._network = network
        self._neighbours: List[Peer] = self._get_neighbours()
        self._reply_terminated = False
        self.replied_peers: List[Peer] = []
        self._reply_lock = threading.Lock()
        self._lookup_lock = threading.Lock()

    @staticmethod
    def get_product_enum(product):
        return Item(product)

    def _get_neighbours(self):
        neighbours = self._current_peer.neighbours

        res = []
        for neighbour_id in neighbours:
            neighbour_peer_obj = self._network[neighbour_id]
            res.append(neighbour_peer_obj)
        return res

    def lookup(self, buyer_id: int, product, hop_count: int, search_path: List[str]):
        with self._lookup_lock:
            product = self.get_product_enum(product)

            LOGGER.info(f"Lookup call buyer id: {buyer_id}, peer id {self._current_peer.id}, product: {product}, "
                        f"hop_count: {hop_count}, search_path: {search_path}")

            # Return if max hop count is reached
            if hop_count >= self._current_peer.max_hop_count:
                LOGGER.info(f"Max hop count reached for Buyer: {buyer_id}")
                LOGGER.info(f"Max hop count reached at Peer: {self._current_peer.id},Type: {self._current_peer.type} ")
                return

            # Execute lookup on neighbours
            for neighbour in self._neighbours:
                rpc_conn = self._get_rpc_connection(neighbour)

                # Skip look up request if already sent
                if neighbour.id in search_path:
                    continue

                search_path.append(neighbour.id)
                LOGGER.info(f"Calling neighbour {neighbour.id}, search_path {search_path}")

                neighbour_search_path = search_path[:]
                func_to_execute = lambda: rpc_conn.lookup(buyer_id, product, hop_count + 1, neighbour_search_path)
                self._execute_in_thread(func_to_execute)

                search_path = search_path[:-1]

    def reply(self, seller_id, reply_path: List[str]):
        with self._reply_lock:
            LOGGER.info(f"Got reply from {seller_id}. Current peer id is {self._current_peer.id}, "
                        f"Reply path is {reply_path}")

            if len(reply_path) == 1:
                self._reply_terminated = True
                peer = self._network[seller_id]
                self.replied_peers.append(peer)
                return

            neighbour_id = reply_path[-2]  # 1
            neighbour_obj = self._network[neighbour_id]

            rpc_conn = self._get_rpc_connection(neighbour_obj)

            reply_path = reply_path[:-1]
            func_to_execute = lambda: rpc_conn.reply(seller_id, reply_path)
            self._execute_in_thread(func_to_execute)

    def _execute_in_thread(self, func):
        self._pool.submit(func)

    @staticmethod
    def _get_rpc_connection(neighbour: Peer):
        return RpcHelper(host=neighbour.host,
                         port=neighbour.port).get_client_connection()

    def shutdown(self):
        self._pool.shutdown()
