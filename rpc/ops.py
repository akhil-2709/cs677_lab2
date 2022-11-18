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
from trader_list import TraderList
from utils import get_new_item

LOGGER = get_logger(__name__)


@Pyro4.expose
class CommonOps(ABC):
    def __init__(self,
                 network: Dict[str, Peer],
                 current_peer: Peer,
                 item_quantities_map: dict,
                 trader_obj: TraderList,
                 thread_pool_size=20
                 ):
        self.item_quantities_map = item_quantities_map
        self._current_peer = current_peer
        self._pool = ThreadPoolExecutor(max_workers=thread_pool_size)
        self._network = network
        self._neighbours: List[Peer] = self._get_neighbours()
        self._reply_terminated = False
        self.replied_peers: List[Peer] = []
        self._reply_lock = threading.Lock()
        self._lookup_lock = threading.Lock()
        self._buy_lock = threading.Lock()
        self._trader_obj = trader_obj
        self._trader_list = []

    @staticmethod
    def get_product_enum(product):
        return Item(product)

    @staticmethod
    def get_seller_obj(self, seller_id):
        return self._network[seller_id]

    def get_product_price(self, product):
        quantity, price = self._item_quantities_map[product]
        return price

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

    def update_seller(self, seller_id):
        selected_seller_obj = self._network[seller_id]
        #selected_seller_obj.lamport = max(self._current_peer.lamport, selected_seller_obj.lamport) + 1

        if self._check_if_item_available(selected_seller_obj.item):
            LOGGER.info(f"Item {selected_seller_obj.item} is available and seller is {selected_seller_obj.id}")
            price = self.get_product_price(selected_seller_obj.item)
            self._current_peer.commission -= .20 * price
            selected_seller_obj.quantity -= 1
            LOGGER.info(
                f" Trader sold {selected_seller_obj.item} from seller {seller_id} and quantity: {selected_seller_obj.quantity} remains now")

            selected_seller_obj.amt_earned += price
            if selected_seller_obj.quantity <= 0:
                old_item = selected_seller_obj.item
                item = get_new_item(current_item=self._current_peer.item)
                quantity, price = self._item_quantities_map[item]
                selected_seller_obj.quantity = quantity
                selected_seller_obj.item = item
                LOGGER.info(
                    f"Item {old_item} sold! Seller {seller_id} is now selling {item} and has a quantity {quantity}")
                raise ValueError(f"Could not execute Buy order for item {item}")

                rpc_conn = self._get_rpc_connection()
                execute_function = rpc_conn.register_products(seller_id=seller_id)
                self._execute_in_thread(execute_function)

    def update_buyer(self, buyer_id):
        buyer_obj = self._network[buyer_id]
       # buyer_obj.lamport = max(self._current_peer.lamport, buyer_obj.lamport) + 1
        buyer_obj.quantity += 1
       # buyer_obj.amt_spent += self.get_product_price(buyer_obj.item)

    def buy(self, buyer_id: str, product: Item):
        with self._buy_lock:
            product = self.get_product_enum(product)
            LOGGER.info(f"Buy call Buyer: {buyer_id}, trader id {self._current_peer.id}, product: {product}")
            buyer_obj = self._network[buyer_id]
            LOGGER.info(f"buyer clock : {buyer_obj.lamport}")
            LOGGER.info(f"trader clock : {self._current_peer.lamport}")
            buyer_lamport = buyer_obj.lamport
            trader_lamport = self._current_peer.lamport
            LOGGER.info(f"buyer_lamport : {buyer_lamport}")
            LOGGER.info(f"trader_lamport : {trader_lamport}")

            if buyer_lamport > trader_lamport:
                self._current_peer.lamport(buyer_lamport+1)
            else:
                self._current_peer.lamport(trader_lamport+1)

            LOGGER.info(f" after buyer_lamport : {buyer_lamport}")
            LOGGER.info(f" after trader_lamport : {trader_lamport}")

           # self._current_peer.lamport(max(buyer_obj.lamport, self._current_peer.lamport)+1)
            LOGGER.info(f" after trader clock : {self._current_peer.lamport}")

            self._trader_list = self._trader_obj.get_trader_list()
            if self._trader_list:
                LOGGER.info(f"trader list : {self._trader_list}")
                sorted(self._trader_list, key=lambda x: x[1])

                LOGGER.info(f"sorted list : {self._trader_list}")

                for seller, lamport_clock in self._trader_list:
                    product = self.get_product_enum(product)
                    LOGGER.info(f"product : {product}")
                    LOGGER.info(f"seller_obj.item : {seller.item}")
                    if seller.item == product:
                        seller_id = seller
                        LOGGER.info(f"seller selected  : {seller.id}")
                        break
                self._current_peer.lamport += 1

                seller.print()
                buyer_obj.print()

                rpc_conn = self._get_rpc_connection(buyer_id)
                func_to_execute1 = lambda: rpc_conn.update_buyer(buyer_id)
                self._execute_in_thread(func_to_execute1)
                func_to_execute2 = lambda: rpc_conn.update_seller(seller_id)
                self._execute_in_thread(func_to_execute2)

    def register_products(self, seller_id):
        seller_obj = self._network[seller_id]
        # seller_obj.lamport+=1
        LOGGER.info(f"seller ID : {seller_id}")
        LOGGER.info(f"seller clock : {seller_obj.lamport}")
        LOGGER.info(f"_trader_list : {self._trader_list}")
        print("_trader_list", self._trader_list)
        self._trader_obj.set_trader_list(seller_id)
        LOGGER.info(f" after setting _trader_list : {self._trader_list}")

    def _execute_in_thread(self, func):
        self._pool.submit(func)

    @staticmethod
    def _get_rpc_connection(neighbour: Peer):
        return RpcHelper(host=neighbour.host,
                         port=neighbour.port).get_client_connection()

    def _check_if_item_available(self, product: Item):
        return self._current_peer.item == product and self._current_peer.quantity > 0

    def shutdown(self):
        self._pool.shutdown()
