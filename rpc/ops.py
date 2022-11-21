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
                 thread_pool_size=20,

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

    def update_seller(self, trader_id, product):
        with self._peer_writer.peers_lock:
            network_dict = self._peer_writer.get_peers()
            LOGGER.info(f"Updating seller: {network_dict[str(self._current_peer.id)]}")

            network_dict[str(self._current_peer.id)]['lamport'] = max(
                network_dict[str(self._current_peer.id)]['lamport'], network_dict[str(trader_id)]['lamport']) + 1

            price = self.get_product_price(network_dict[str(self._current_peer.id)]['item'])
            network_dict[str(trader_id)]['commission'] += .2 * price
            network_dict[str(self._current_peer.id)]['quantity'] -= 1
            network_dict[str(self._current_peer.id)]['amt_earned'] += price * .8

            LOGGER.info(
                f" Trader sold {network_dict[str(self._current_peer.id)]['item']} from seller {network_dict[str(self._current_peer.id)]['id']} and quantity: {network_dict[str(self._current_peer.id)]['quantity']} remains now")
            LOGGER.info(f"Seller Object Updated : {network_dict[str(self._current_peer.id)]}")

            self._peer_writer.write_peers(network_dict)

            if network_dict[str(self._current_peer.id)]['quantity'] <= 0:
                network_dict = json_files.json_ops.get_peers()

                LOGGER.info(f"Seller.quantity: {network_dict[str(self._current_peer.id)]['quantity']}")

                sellers_list = self._peer_writer.get_sellers()

                LOGGER.info(f" Before Trader list: {sellers_list}")
                sellers_list.pop(0)
                LOGGER.info(f"After Trader list: {sellers_list}")

                self._peer_writer.write_sellers(sellers_list)

                old_item = network_dict[str(self._current_peer.id)]['item']
                item = get_new_item(current_item=network_dict[str(self._current_peer.id)]['item'])
                quantity, price = self._item_quantities_map[item]
                network_dict[str(self._current_peer.id)]['quantity'] = quantity
                network_dict[str(self._current_peer.id)]['item'] = item
                LOGGER.info(
                    f"Item {old_item} id sold! Seller {network_dict[str(self._current_peer.id)]['id']} is now selling {network_dict[str(self._current_peer.id)]['item']} and has a "
                    f"quantity {network_dict[str(self._current_peer.id)]['quantity']}")

                self._peer_writer.write_peers(network_dict)

                rpc_conn = self._get_rpc_connection(network_dict[str(trader_id)])
                execute_function = rpc_conn.register_products(self._current_peer.id,
                                                              network_dict[str(self._current_peer.id)]['lamport'])
                self._execute_in_thread(execute_function)
                raise ValueError(f"Could not execute Buy order for item {item}")

    def update_buyer(self, trader_id):
        with self._peer_writer.peers_lock:
            LOGGER.info(f"Updating buyer: {self._current_peer.id}")
            network_dict = self._peer_writer.get_peers()

            LOGGER.info(f" buyer obj : {network_dict[str(self._current_peer.id)]}")
            LOGGER.info(f" trader obj : {network_dict[str(trader_id)]}")

            network_dict[str(self._current_peer.id)]['lamport'] = max(self._current_peer.lamport,
                                                                      network_dict[str(self._current_peer.id)][
                                                                          'lamport']) + 1

            LOGGER.info(f" after  buyer obj : {network_dict[str(self._current_peer.id)]}")
            LOGGER.info(f" after trader obj : {network_dict[str(trader_id)]}")

            network_dict[str(self._current_peer.id)]['quantity'] = network_dict[str(self._current_peer.id)][
                                                                       'quantity'] + 1

            network_dict[str(self._current_peer.id)]['amt_spent'] = self.get_product_price(
                network_dict[str(self._current_peer.id)]['item'])

            self._peer_writer.write_peers(network_dict)
            LOGGER.info(f" Buyer amt spent : {network_dict[str(self._current_peer.id)]['amt_spent']}")

            LOGGER.info(f"Updated buyer: {network_dict[str(self._current_peer.id)]}")

    def buy(self, buyer_id: str, product: Item, buyer_clock: int):
        with self._peer_writer.peers_lock:
            network_dict = self._peer_writer.get_peers()
            product = self.get_product_enum(product).value
            LOGGER.info(f"network_dict: {network_dict}")
            LOGGER.info(f"buyer id: {network_dict[str(buyer_id)]['id']}")
            LOGGER.info(f"trader id: {network_dict[str(self._current_peer.id)]['id']}")
            LOGGER.info(
                f"Buy call by Buyer: {network_dict[str(buyer_id)]['id']} on Trader: {network_dict[str(self._current_peer.id)]['id']} for product: {product}")

            LOGGER.info(f" Before Buyer Object : {network_dict[str(buyer_id)]}")
            LOGGER.info(f" Before Trader Object : {network_dict[str(self._current_peer.id)]}")

            network_dict[str(self._current_peer.id)]['lamport'] = max(network_dict[str(buyer_id)]['lamport'],
                                                                      network_dict[str(self._current_peer.id)][
                                                                          'lamport']) + 1
            LOGGER.info(f" After Buyer Object : {network_dict[str(buyer_id)]}")
            LOGGER.info(f" After Trader Object : {network_dict[str(self._current_peer.id)]}")

            sellers_list = self._peer_writer.get_sellers()

            traders_list = []
            for seller in sellers_list:
                traders_list.append((network_dict[str(seller)]['id'], network_dict[str(seller)]['lamport']))

            LOGGER.info(f"traders_list: {traders_list}")

            if traders_list:
                LOGGER.info(f"Trader list : {traders_list}")
                sorted(traders_list, key=lambda x: x[1])
                selected_seller = ""
                for seller, seller_clock in traders_list:
                    if self._check_if_item_available(str(product), network_dict[str(seller)]['item'],
                                                     network_dict[str(seller)]['quantity']):
                        LOGGER.info(
                            f"Item {network_dict[str(seller)]['item']} is available and seller is {network_dict[str(seller)]['id']}")
                        selected_seller = network_dict[str(seller)]['id']
                        break
                if not selected_seller:
                    LOGGER.info(f" No seller found")
                    raise ValueError(f"Ask buyer to buy diff item")

                network_dict[str(self._current_peer.id)]['lamport'] += 1

                LOGGER.info(f" After selecting seller: Buyer Object : {network_dict[str(buyer_id)]['id']}")
                LOGGER.info(
                    f" After selecting seller: Trader Object : {network_dict[str(self._current_peer.id)]['id']}")

                self._peer_writer.write_peers(network_dict)

                rpc_conn = self._get_rpc_connection(network_dict[str(buyer_id)])
                func_to_execute1 = lambda: rpc_conn.update_buyer(self._current_peer.id)
                self._execute_in_thread(func_to_execute1)
                sleep(2)
                rpc_conn2 = self._get_rpc_connection(network_dict[str(selected_seller)])
                func_to_execute2 = lambda: rpc_conn2.update_seller(self._current_peer.id, product)
                self._execute_in_thread(func_to_execute2)

                LOGGER.info("After update_seller call()")

    def register_products(self, seller_id, seller_clock):
        with self._peer_writer.peers_lock:
            network_dict = self._peer_writer.get_peers()

            LOGGER.info(f"Inside register_products()")
            LOGGER.info(f"Before seller obj: {network_dict[str(seller_id)]}")
            LOGGER.info(f"Before trader obj: {network_dict[str(self._current_peer.id)]}")

            network_dict[str(self._current_peer.id)]['lamport'] = max(seller_clock,
                                                                      network_dict[str(self._current_peer.id)][
                                                                          'lamport']) + 1

            self._peer_writer.write_peers(network_dict)

            LOGGER.info(f"After seller obj: {network_dict[str(seller_id)]}")
            LOGGER.info(f"After trader obj: {network_dict[str(self._current_peer.id)]}")

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
    def _get_rpc_connection(neighbour: dict):
        return RpcHelper(host=neighbour['host'],
                         port=neighbour['port']).get_client_connection()

    @staticmethod
    def _check_if_item_available(product, seller_item, seller_quantity):
        return seller_item == product and seller_quantity > 0

    def shutdown(self):
        self._pool.shutdown()
