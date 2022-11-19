import threading
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from time import sleep
from typing import List, Dict

import Pyro4

from enums.item_type import Item
from logger import get_logger
from peer.model import Peer
from rpc.rpc_helper import RpcHelper
# from trader_list import TraderList
from utils import get_new_item
from config import trader_list

LOGGER = get_logger(__name__)


@Pyro4.expose
class CommonOps(ABC):
    def __init__(self,
                 network: Dict[str, Peer],
                 current_peer: Peer,
                 item_quantities_map: dict,
                 # trader_obj: TraderList,
                 thread_pool_size=20
                 ):
        self._item_quantities_map = item_quantities_map
        self._current_peer = current_peer
        self._pool = ThreadPoolExecutor(max_workers=thread_pool_size)
        self._network = network
        self._neighbours: List[Peer] = self._get_neighbours()
        self._reply_terminated = False
        self.replied_peers: List[Peer] = []
        self._reply_lock = threading.Lock()
        self._lookup_lock = threading.Lock()
        self._buy_lock = threading.Lock()
        self._update_seller_lock = threading.Lock()
        self._update_buyer_lock = threading.Lock()
        self._register_product= threading.Lock()
        # self._trader_obj = trader_obj
        # self._trader_list = []

    @staticmethod
    def get_product_enum(product):
        return Item(product)

    @staticmethod
    def get_seller_obj(self, seller_id):
        return self._network[seller_id]

    def get_product_price(self, product):
        product = self.get_product_enum(product)
        quantity, price = self._item_quantities_map[product]
        return price

    def _get_neighbours(self):
        neighbours = self._current_peer.neighbours

        res = []
        for neighbour_id in neighbours:
            neighbour_peer_obj = self._network[neighbour_id]
            res.append(neighbour_peer_obj)
        return res

    def update_seller(self, trader_id, product,trader_list):
        with self._update_seller_lock:
            #seller_obj = self._network[self._current_peer.id]

            LOGGER.info(f"Updating seller: {self._current_peer.id}")
            trader_obj = self._network[trader_id]
            self._current_peer.lamport = max(self._current_peer.lamport, trader_obj.lamport) + 1

            price = self.get_product_price(self._current_peer.item)
            self._current_peer.commission -= .20 * price
            self._current_peer.quantity -= 1
            self._current_peer.amt_earned += price

            LOGGER.info(
                f" Trader sold {self._current_peer.item} from seller {self._current_peer.id} and quantity: {self._current_peer.quantity} remains now")
            LOGGER.info(f"Seller Object Updated : {self._current_peer}")

            if self._current_peer.quantity <= 0:
                LOGGER.info(f"Seller.quantity: {self._current_peer.quantity}")
                LOGGER.info(f" Before Trader list: {trader_list}")
                trader_list.pop(0)
                LOGGER.info(f"After Trader list: {trader_list}")
                old_item = self._current_peer.item
                item = get_new_item(current_item=self._current_peer.item)
                quantity, price = self._item_quantities_map[item]
                self._current_peer.quantity = quantity
                self._current_peer.item = item
                LOGGER.info(
                    f"Item {old_item} id sold! Seller {self._current_peer.id} is now selling {self._current_peer.item} and has a "
                    f"quantity {self._current_peer.quantity}")

                LOGGER.info(f"self._network inside update seller: {self._network}")

                rpc_conn = self._get_rpc_connection(trader_obj)
                execute_function = rpc_conn.register_products(self._current_peer.id, self._current_peer.lamport,trader_list)
                self._execute_in_thread(execute_function)
                raise ValueError(f"Could not execute Buy order for item {item}")
                # except Exception as ex:
                #     LOGGER.exception(f"Failed to execute update_seller")
                #     LOGGER.exception(f"exception: {ex}")

    def update_buyer(self, trader_id):
        with self._update_buyer_lock:
            LOGGER.info(f"Updating buyer: {self._current_peer.id}")
            trader_obj = self._network[trader_id]

            LOGGER.info(f" buyer obj : {self._current_peer_obj}")
            LOGGER.info(f" trader obj : {trader_obj}")

            self._current_peer.lamport = max(self._current_peer.lamport, trader_obj.lamport) + 1

            LOGGER.info(f" after buyer obj : {self._current_peer}")
            LOGGER.info(f" after trader obj : {trader_obj}")

            self._current_peer.quantity = self._current_peer.quantity + 1
            self._current_peer.amt_spent = self.get_product_price(self._current_peer.item)

            LOGGER.info(f" Buyer amt spent : {self._current_peer.amt_spent}")
            LOGGER.info(f"trader_list: {trader_list}")
            LOGGER.info(f"Updated buyer: {self._current_peer}")

    def buy(self, buyer_id: str, product: Item, buyer_clock: int):
        with self._buy_lock:
            product = self.get_product_enum(product)

            LOGGER.info(f"Buy call by Buyer: {buyer_id} on Trader: {self._current_peer.id} for product: {product}")
            buyer_obj = self._network[buyer_id]
            #trader_obj = self._current_peer

            LOGGER.info(f" Before Trader Object : {buyer_obj}")
            LOGGER.info(f" Before Buyer Object : {self._current_peer}")

            self._current_peer.lamport = max(buyer_clock, self._current_peer.lamport) + 1

            LOGGER.info(f" After Trader Object : {buyer_obj}")
            LOGGER.info(f" After Buyer Object : {self._current_peer}")

            if trader_list:
                LOGGER.info(f"Trader list : {trader_list}")
                sorted(trader_list, key=lambda x: x[1])

                for seller, seller_clock in trader_list:
                    seller_obj = self._network[seller]
                    if self._check_if_item_available(product, seller_obj):
                        LOGGER.info(f"Item {seller_obj.item} is available and seller is {seller_obj.id}")
                        break
                    else:
                        LOGGER.info(f" No seller found")
                self._current_peer.lamport = self._current_peer.lamport + 1

                LOGGER.info(f" After selecting seller: Trader Object : {buyer_obj}")
                LOGGER.info(f" After selecting seller: Buyer Object : {self._current_peer}")

                rpc_conn = self._get_rpc_connection(buyer_obj)
                func_to_execute1 = lambda: rpc_conn.update_buyer(self._current_peer.id)
                self._execute_in_thread(func_to_execute1)
                sleep(2)
                rpc_conn2 = self._get_rpc_connection(seller_obj)
                func_to_execute2 = lambda: rpc_conn2.update_seller(self._current_peer.id, product,trader_list)
                self._execute_in_thread(func_to_execute2)

    def register_products(self, seller_id, seller_clock,trader_list):

        LOGGER.info(f"self._network inside register: {self._network}")
        seller_obj = self._network[seller_id]
        LOGGER.info(f"Inside register_products()")
        LOGGER.info(f"Before seller obj: {seller_obj}")
        LOGGER.info(f"Before trader obj: {self._current_peer}")

        self._current_peer.lamport = max(seller_clock, self._current_peer.lamport) + 1

        LOGGER.info(f"After seller obj: {seller_obj}")
        LOGGER.info(f"After trader obj: {self._current_peer}")

        LOGGER.info(f"Trader_list : {trader_list}")
        trader_list.append((seller_id, seller_obj.lamport))
        LOGGER.info(f"After updating Trader_list : {trader_list}")

    def _execute_in_thread(self, func):
        self._pool.submit(func)

    @staticmethod
    def _get_rpc_connection(neighbour: Peer):
        return RpcHelper(host=neighbour.host,
                         port=neighbour.port).get_client_connection()

    def _check_if_item_available(self, product, seller_obj):
        return seller_obj.item == product and seller_obj.quantity > 0

    def shutdown(self):
        self._pool.shutdown()
