import threading
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import List, Dict

import Pyro4

import json_files.json_ops
from enums.item_type import Item
from logger import get_logger
from peer.model import Peer
from rpc.rpc_helper import RpcHelper
from utils import get_new_item
from json_files.json_ops import PeerWriter

LOGGER = get_logger(__name__)


@Pyro4.expose
class CommonOps(ABC):
    def __init__(self,
                 network: Dict[str, Peer],
                 current_peer: Peer,
                 peer_writer: PeerWriter,
                 item_quantities_map: dict,
                 thread_pool_size=30,

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
        self._register_product = threading.Lock()
        self._read_write_lock = threading.Lock()
        self._peer_writer = peer_writer

    @staticmethod
    def get_product_enum(product):
        return Item(product)

    @staticmethod
    def get_seller_obj(self, seller_id):
        return self._network[seller_id]

    def get_product_price(self, product):
        product = self.get_product_enum(product).value
        quantity, price = self._item_quantities_map[product]
        return price

    def _get_neighbours(self):
        neighbours = self._current_peer.neighbours
        res = []
        for neighbour_id in neighbours:
            neighbour_peer_obj = self._network[neighbour_id]
            res.append(neighbour_peer_obj)
        return res

    def update_seller(self, trader_clock):
        with self._peer_writer.peers_lock:
            LOGGER.info(f"Before updating seller clock: {self._current_peer.vect_clock}")

            #self._current_peer.lamport = max(self._current_peer.vect_clock, trader_clock) + 1
            self._current_peer.vect_clock = update_vector_clock(trader_clock, self._current_peer.vect_clock)
            self._current_peer.vect_clock[self._current_peer.id] += 1

            LOGGER.info(f"After updating seller clock: {self._current_peer.vect_clock}")

    def change_item(self, network_dict, seller_id):

        LOGGER.info(f"Seller's quantity: {network_dict[str(seller_id)]['quantity']}")
        sellers_list = self._peer_writer.get_sellers()
        LOGGER.info(f" Before Trader list: {sellers_list}")
        sellers_list.pop(0)
        LOGGER.info(f"After Trader list: {sellers_list}")
        self._peer_writer.write_sellers(sellers_list)

        old_item = network_dict[str(seller_id)]['item']
        item = get_new_item(current_item=network_dict[str(seller_id)]['item'])
        quantity, price = self._item_quantities_map[item]
        network_dict[str(seller_id)]['quantity'] = quantity
        network_dict[str(seller_id)]['item'] = item
        LOGGER.info(
            f"Item {old_item} id sold! Seller {seller_id} is now selling {network_dict[str(seller_id)]['item']} and has a "
            f"quantity {network_dict[str(seller_id)]['quantity']}")

        rpc_conn = self._get_rpc_connection(self._current_peer.host, self._current_peer.port)
        execute_function = rpc_conn.register_products(seller_id, network_dict[str(seller_id)]['vect_clock'])
        self._execute_in_thread(execute_function)
        raise ValueError(f"Could not execute Buy order for item {item}")

    def update_buyer(self, trader_clock):
        with self._peer_writer.peers_lock:
            LOGGER.info(f"Before buyer clock: {self._current_peer.vect_clock}")
        
            #self._current_peer.lamport = max(self._current_peer.lamport, trader_clock) + 1
            self._current_peer.vect_clock = update_vector_clock(trader_clock, self._current_peer.vect_clock)
            self._current_peer.vect_clock[self._current_peer.id] += 1

            LOGGER.info(f"Updated buyer clock: {self._current_peer.vect_clock}")

    def buy(self, buyer_id: str, product: Item, buyer_clock: int):
        with self._read_write_lock:
            product = self.get_product_enum(product).value

            LOGGER.info(
                f"Buy call by buyer: {buyer_id} on trader: {self._current_peer.id} for product: {product}")

            #self._current_peer.lamport = max(self._current_peer.lamport, buyer_clock) + 1
            self._current_peer.vect_clock = update_vector_clock(self._current_peer.vect_clock, buyer_clock)
            self._current_peer.vect_clock[self._current_peer.id] += 1

            sellers_list = self._peer_writer.get_sellers()
            network_dict = self._peer_writer.get_peers()

            LOGGER.info(f"network_dict =============:{network_dict}")

            traders_list = []
            for seller in sellers_list:
                traders_list.append((network_dict[str(seller)]['id'], network_dict[str(seller)]['vect_clock'][int(seller)]))

            LOGGER.info(f"Traders_list: {traders_list}")

            if traders_list:
                sorted(traders_list, key=lambda x: x[1])
                selected_seller = ""
                for seller, seller_clock in traders_list:
                    if self._check_if_item_available(str(product), network_dict[str(seller)]['item'],
                                                     network_dict[str(seller)]['quantity']):
                        LOGGER.info(
                            f"Item {network_dict[str(seller)]['item']} is available and seller is {seller}")

                        # update the buyer details after transaction
                        network_dict[str(buyer_id)]['quantity'] += 1
                        network_dict[str(buyer_id)]['amt_spent'] += self.get_product_price(product)
                        LOGGER.info(
                            f"Buyer spent amount: {self.get_product_price(product)} to buy: {product} and the current "
                            f"quantity: {network_dict[str(buyer_id)]['quantity']}")

                        # update the seller's details after transaction
                        price = self.get_product_price(product)
                        self._current_peer.commission += .2 * price
                        network_dict[str(seller)]['quantity'] -= 1
                        network_dict[str(seller)]['amt_earned'] += price * .8
                        LOGGER.info(
                            f" Trader sold {product} from seller {selected_seller} and it earned amount:"
                            f" {network_dict[str(seller)]['amt_earned']} and quantity: {network_dict[str(seller)]['quantity']} remains now")

                        if network_dict[str(seller)]['quantity'] <= 0:
                            self.change_item(network_dict, seller)

                        sellers_list = self._peer_writer.write_sellers(sellers_list)
                        network_dict = self._peer_writer.write_peers(network_dict)

                        # increment trader's clock
                        #self._current_peer.lamport += 1
                        self._current_peer.vect_clock[self._current_peer] += 1

                        # update the buyer's clock
                        rpc_conn = self._get_rpc_connection(network_dict[str(buyer_id)]['host'],
                                                            network_dict[str(buyer_id)]['port'])
                        func_to_execute1 = lambda: rpc_conn.update_buyer(self._current_peer.vect_clock)
                        self._execute_in_thread(func_to_execute1)
                        sleep(2)
                        # update the seller's clock
                        rpc_conn2 = self._get_rpc_connection(network_dict[str(selected_seller)]['host'],
                                                             network_dict[str(selected_seller)]['port'])
                        func_to_execute2 = lambda: rpc_conn2.update_seller(self._current_peer.vect_clock)
                        self._execute_in_thread(func_to_execute2)
                        break
                if not selected_seller:
                    LOGGER.info(f"No seller found")
                    raise ValueError(f"Ask buyer to buy different item")

    def register_products(self, seller_id, seller_clock):
        with self._register_product:
            network_dict = self._peer_writer.get_peers()

            LOGGER.info(f"Inside register_products()")

            # update trader's clock
            #self._current_peer.lamport = max(seller_clock, self._current_peer.lamport) + 1
            self._current_peer.vect_clock = update_vector_clock(seller_clock, self._current_peer.vect_clock)
            self._current_peer.vect_clock[self._current_peer.id] += 1

            self._peer_writer.write_peers(network_dict)

            # update seller's list
            seller_list = self._peer_writer.get_sellers()
            LOGGER.info(f"Read seller_list: {seller_list}")
            seller_list.append(str(seller_id))
            self._peer_writer.write_sellers(seller_list)
            LOGGER.info(f"Updated seller_list: {seller_list}")

    def _execute_in_thread(self, func):
        self._pool.submit(func)

    def get_trader_list(self):
        network_dict = self._peer_writer.get_peers()
        for key in network_dict:
            if network_dict[key]['type'] == "SELLER":
                pass

    @staticmethod
    def _get_rpc_connection(host, port):
        LOGGER.info(f"host {host}, port {port}")
        return RpcHelper(host=host,
                         port=port).get_client_connection()

    @staticmethod
    def _check_if_item_available(product, seller_item, seller_quantity):
        return seller_item == product and seller_quantity > 0

    def shutdown(self):
        self._pool.shutdown()

def update_vector_clock(list1, list2):
    lst =[]
    for i in range(len(list1)):
        lst.append(max(list1[i], list2[i]))
    return lst