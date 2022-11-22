import multiprocessing
import threading
from threading import Thread
from time import sleep
from typing import Dict

import Pyro4

import config
import json_files.json_ops
from config import params_pickle_file_path
from enums.peer_type import PeerType
from logger import get_logger
from peer.model import Peer
from rpc.ops_factory import OpsFactory
from rpc.rpc_helper import RpcHelper
from utils import pickle_load_from_file, get_new_item
from concurrent.futures import ThreadPoolExecutor

LOGGER = get_logger(__name__)

data_lock = threading.Lock()

pool = ThreadPoolExecutor(max_workers=20)
leader_lock = threading.Lock()

peers_lock = multiprocessing.Lock()
sellers_lock = multiprocessing.Lock()
# traderChanged = multiprocessing.Value(False)

traderChanged = False


def elect_leader(trader_id, current_peer_obj, peer_writer):
    with multiprocessing.Lock():
        LOGGER.info(f"Peer {current_peer_obj.id} initiated the election")
        LOGGER.info(f"Current leader before the election is: {trader_id}")
        network_dict = peer_writer.get_peers()
        current_id = current_peer_obj.id
        connection_successful = []
        for peer_id in  current_peer_obj.neighbours:
            if peer_id != current_peer_obj.trader:
                uri = f"PYRO:shop@{network_dict[str(peer_id)]['host']}:{network_dict[str(peer_id)]['port']}"
                with Pyro4.Proxy(uri) as p:
                    try:
                        p._pyroBind()
                        LOGGER.info("I am OK")
                        connection_successful.append(peer_id)
                    except:
                        LOGGER.info("No reply from the peer")
        leader = current_peer_obj.trader
        if connection_successful:
            leader = max(connection_successful)

        LOGGER.info(f"New leader Elected: {str(leader)}")
        LOGGER.info(f"I am the New Leader: {leader}")

        # update trader for everyone
        current_peer_obj.trader = leader
        global traderChanged
        traderChanged = True

        # update the new leader's type
        if current_peer_obj.id == leader:
            current_peer_obj.type = PeerType.TRADER

def checking_liveliness(current_peer_obj, peer_writer):
    LOGGER.info(f"Checking liveliness of the Trader")
    trader_id = current_peer_obj.trader
    LOGGER.info(f" trader: {trader_id}")
    trader_host = current_peer_obj.trader_host
    trader_port = current_peer_obj.trader_port
    uri = f"PYRO:shop@{trader_host}:{trader_port}"
    with Pyro4.Proxy(uri) as p:
        try:
            p._pyroBind()
            LOGGER.info("Trader is OK")
        except:
            LOGGER.info("No reply from trader")
            # Initiate the Election when there is no reply
            elect_leader(trader_id, current_peer_obj, peer_writer)


def handle_process_start(ops, current_peer_obj: Peer, network_map: Dict[str, Peer], peer_writer):
    current_id = current_peer_obj.id
    LOGGER.info(f"Start process called for peer {current_id}. Sleeping!")
    sleep(10)

    if current_peer_obj.type == PeerType.BUYER:
        LOGGER.info(f"Waiting for sellers to register their products with the trader...Sleeping!")
        sleep(5)
        LOGGER.info(f"Initializing buyer flow for peer {current_id}")
        while True:
            # checking liveliness of the trader
            checking_liveliness(current_peer_obj, peer_writer)
            current_item = current_peer_obj.item
            LOGGER.info(f"Buyer {current_id} sending buy request for item {current_item}")
            try:
                current_peer_obj.vect_clock[current_peer_obj.id] += 1
                LOGGER.info(f"Incrementing buyer clock: {current_peer_obj.vect_clock}")

                trader_id = current_peer_obj.trader
                LOGGER.info(f" Buyer is requesting the item from the trader : {trader_id}")
                trader_host = current_peer_obj.trader_host
                trader_port = current_peer_obj.trader_port
                helper = RpcHelper(host=trader_host, port=trader_port)

                LOGGER.info(f" Trader host {trader_host} , Trader_port {trader_port},"
                            f"current buyer clock {current_peer_obj.vect_clock}")

                trader_connection = helper.get_client_connection()
                trader_connection.buy(current_id, current_item, current_peer_obj.vect_clock, traderChanged)
                LOGGER.info(f"Transaction is complete")

            except ValueError as e:
                LOGGER.info("No seller found. Please request for another item")
                LOGGER.info(f"Opting new item!")
                new_item = get_new_item(current_item=current_item)
                LOGGER.info(f"Buyer {current_id} buying new item {new_item}. "
                            f"Old item was {current_item}!")
                current_peer_obj.item = new_item
            except Exception as ex:
                LOGGER.exception(f"Failed to execute buy call")

    if current_peer_obj.type == PeerType.SELLER:

        trader_id = current_peer_obj.trader
        trader_host = current_peer_obj.trader_host
        trader_port = current_peer_obj.trader_port
        LOGGER.info(f"Seller: {current_peer_obj.id} is registering items with the trader: {trader_id}")

        # incrementing seller's vector clock
        current_peer_obj.vect_clock[current_peer_obj.id] += 1
        LOGGER.info(f"Seller clock after this local event:  {current_peer_obj.vect_clock}")

        helper = RpcHelper(host=trader_host, port=trader_port)
        trader_connection = helper.get_client_connection()
        trader_connection.register_products(current_peer_obj.id, current_peer_obj.vect_clock, traderChanged)

        LOGGER.info(f" Seller successfully registered its items ")

        # Checking the liveliness of the trader
        # while True:
        #     trigger_thread = Thread(target=checking_liveliness, args=(current_peer_obj, peer_writer))
        #     trigger_thread.start()
        #     sleep(2)

def start_process(current_peer_id: int):
    peer_writer = json_files.json_ops.PeerWriter(peers_lock=peers_lock, sellers_lock=sellers_lock)
    data = pickle_load_from_file(params_pickle_file_path)

    network_map = data["network_map"]
    item_quantities_map = config.item_quantities_map
    thread_pool_size = config.thread_pool_size

    current_peer_obj: Peer = network_map[current_peer_id]

    ops_obj = OpsFactory.get_ops(network=network_map,
                                 current_peer=current_peer_obj,
                                 item_quantities_map=item_quantities_map,
                                 thread_pool_size=thread_pool_size,
                                 peer_writer=peer_writer)

    trigger_thread = Thread(target=handle_process_start, args=(ops_obj, current_peer_obj, network_map, peer_writer))
    trigger_thread.start()

    helper = RpcHelper(host=current_peer_obj.host, port=current_peer_obj.port)

    helper.start_server(ops_obj, current_peer_obj.id)

    LOGGER.info(f"Done with processing :{current_peer_obj}")
    ops_obj.shutdown()
