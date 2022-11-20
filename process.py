import multiprocessing
import threading
from threading import Thread
from time import sleep
from typing import Dict

import Pyro4

import config
import csv_files.csv_ops
from config import params_pickle_file_path
from enums.peer_type import PeerType
from logger import get_logger
from peer.model import Peer
from rpc.ops_factory import OpsFactory
from rpc.rpc_helper import RpcHelper
from utils import pickle_load_from_file, get_new_item
from concurrent.futures import ThreadPoolExecutor
from csv_files.csv_ops import PeerWriter

LOGGER = get_logger(__name__)

data_lock = threading.Lock()

pool = ThreadPoolExecutor(max_workers=20)
leader_lock = threading.Lock()

file_write_lock = multiprocessing.Lock()
election_lock = multiprocessing.Lock()

# peer_writer = PeerWriter(lock=file_write_lock)

def elect_leader(trader_id,current_peer_obj,peer_writer):
    with election_lock:
        LOGGER.info(f"called elect_leader()")
        LOGGER.info(f"Current leader: {trader_id}")
        network_dict = peer_writer.get_peers()
        leader = -1
        for peer_id, peer_dict in network_dict.items():
            if int(peer_id) != 5:
                if int(peer_id) > leader:
                    leader = int(peer_id)
        if str(trader_id) in network_dict:
            del network_dict[str(trader_id)]

        peer_writer.write_peers(network_dict)
        network_dict = peer_writer.get_peers()

        LOGGER.info(f"After election: {leader}")

        for peer_id, peer_dict in network_dict.items():
            network_dict[peer_id]['trader'] = leader

        # update the new leader's type
        LOGGER.info(f"current peer obj: {current_peer_obj}")
        if current_peer_obj.id == leader:
            LOGGER.info("current peer object is leader")
            current_peer_obj.type = PeerType.TRADER
            network_dict[str(leader)]['type'] = "TRADER"

        # update trader for everyone
        current_peer_obj.trader = leader

        peer_writer.write_peers(network_dict)

        LOGGER.info(f"New leader elected: {str(leader)}")

def checking_liveliness(current_peer_obj,peer_writer):

    network_dict = peer_writer.get_peers()
    LOGGER.info(f"Checking liveliness")
    trader_id = network_dict[str(current_peer_obj.id)]['trader']
    LOGGER.info(f" trader: {trader_id}")
    trader_dict = network_dict[str(trader_id)]
    trader_host = trader_dict['host']
    trader_port = trader_dict['port']
    helper = RpcHelper(host=trader_host, port=trader_port)
    trader_conn = helper.get_client_connection()
    uri = f"PYRO:shop@{trader_host}:{trader_port}"
    LOGGER.info(f"Check trader conn: {trader_conn}")
    with Pyro4.Proxy(uri) as p:
        try:
            p._pyroBind()
            LOGGER.info("connection successfull")
        except:

            LOGGER.info("connection failure")
            elect_leader(trader_id,current_peer_obj,peer_writer)

def handle_process_start(ops, current_peer_obj: Peer, network_map: Dict[str, Peer], peer_writer):
    current_id = current_peer_obj.id
    LOGGER.info(f"Start process called for peer {current_id}. Sleeping!")
    sleep(10)

    if current_peer_obj.type == PeerType.BUYER:
        sleep(5)
        LOGGER.info(f"Initializing buyer flow for peer {current_id}")
        while True:
            network_dict = peer_writer.get_peers()

            checking_liveliness(current_peer_obj,peer_writer)

            current_item = network_dict[str(current_peer_obj.id)]['item']
            LOGGER.info(f"Buyer {current_id} sending buy request for item {current_item}")
            try:
                # TO DO: change the trader after leader election
                network_dict[str(current_peer_obj.id)]['lamport'] += 1
                LOGGER.info(f"Incrementing buyer clock: {network_dict[str(current_peer_obj.id)]['lamport']}")

                trader_id = network_dict[str(current_peer_obj.id)]['trader']
                LOGGER.info(f" trader: {trader_id}")
                trader_dict = network_dict[str(trader_id)]
                trader_host = trader_dict['host']
                trader_port = trader_dict['port']
                helper = RpcHelper(host=trader_host, port=trader_port)

                LOGGER.info(f" trader host {trader_host} , trader_port {trader_port}, "
                            f"buyer clock {network_dict[str(current_peer_obj.id)]['lamport']}"
                            f"trader clock {trader_dict['lamport']}")

                peer_writer.write_peers(network_dict)

                trader_connection = helper.get_client_connection()
                trader_connection.buy(current_id, current_item, current_peer_obj.lamport)
                LOGGER.info(f" After buy call() trader host {trader_host} , trader_port {trader_port}, "
                            f"buyer clock {network_dict[str(current_peer_obj.id)]['lamport']}"
                            f"trader clock {trader_dict['lamport']}")

            except ValueError as e:
                LOGGER.info("No seller found. Please request for another item")
                LOGGER.info(f"Opting new item!")
                network_dict1 = peer_writer.get_peers()
                new_item = get_new_item(current_item=current_item)
                LOGGER.info(f"Buyer {current_id} buying new item {new_item}. "
                            f"Old item was {current_item}!")
                network_dict1[str(current_id)]['item'] = new_item
                peer_writer.write_peers(network_dict)

            except Exception as ex:
                LOGGER.exception(f"Failed to execute buy call")

    if current_peer_obj.type == PeerType.SELLER:
        network_dict = peer_writer.get_peers()

        LOGGER.info(f"Registering item with the trader {network_dict[str(current_peer_obj.id)]['trader']}")

        network_dict[str(current_peer_obj.id)]['lamport'] += 1

        LOGGER.info(f"Seller clock after this local event:  {network_dict[str(current_peer_obj.id)]['lamport']}")

        trader_id = network_dict[str(current_peer_obj.id)]['trader']
        trader_host = network_dict[str(trader_id)]['host']
        trader_port = network_dict[str(trader_id)]['port']

        peer_writer.write_peers(network_dict)

        helper = RpcHelper(host=trader_host, port=trader_port)
        trader_connection = helper.get_client_connection()

        LOGGER.info(f"Inside seller call():  {network_dict[str(current_peer_obj.id)]['lamport']}")

        trader_connection.register_products(current_peer_obj.id, network_dict[str(current_peer_obj.id)]['lamport'])


def start_process(current_peer_id: int):
    peer_writer = csv_files.csv_ops.PeerWriter(lock=file_write_lock)
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

    LOGGER.info(f"done with processing :{current_peer_obj}")
    ops_obj.shutdown()
