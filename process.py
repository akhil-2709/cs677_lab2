import threading
from threading import Thread
from threading import Thread
from time import sleep
from typing import Dict

import config
import csv_files.csv_ops
from config import params_pickle_file_path
from config import trader
from enums.peer_type import PeerType
from logger import get_logger
from peer.model import Peer
from rpc.ops_factory import OpsFactory
from rpc.rpc_helper import RpcHelper
from utils import pickle_load_from_file, get_new_item

LOGGER = get_logger(__name__)

data_lock = threading.Lock()


def handle_process_start(ops, current_peer_obj: Peer, network_map: Dict[str, Peer]):
    current_id = current_peer_obj.id
    LOGGER.info(f"Start process called for peer {current_id}. Sleeping!")
    sleep(5)

    if current_peer_obj.type == PeerType.BUYER:
        sleep(5)
        LOGGER.info(f"Initializing buyer flow for peer {current_id}")
        while True:
            network_dict = csv_files.csv_ops.get_peers()
            current_item = network_dict[str(current_peer_obj.id)]['item']
            LOGGER.info(f"Buyer {current_id} sending buy request for item {current_item}")
            try:
                # TO DO: change the trader after leader election
                network_dict[str(current_peer_obj.id)]['lamport'] += 1
                LOGGER.info(f"Incrementing buyer clock: {network_dict[str(current_peer_obj.id)]['lamport']}")

                trader_id = network_dict[str(current_peer_obj.id)]['trader']
                LOGGER.info(f" trader: {trader_id}")
                trader_dict = network_dict[str(trader_id)]
                trader_host = trader_dict['_host']
                trader_port = trader_dict['_port']
                helper = RpcHelper(host=trader_host, port=trader_port)

                LOGGER.info(f" trader host {trader_host} , trader_port {trader_port}, "
                            f"buyer clock {network_dict[str(current_peer_obj.id)]['lamport']}"
                            f"trader clock {trader_dict['lamport']}")

                csv_files.csv_ops.write_peers(network_dict)

                trader_connection = helper.get_client_connection()
                trader_connection.buy(current_id, current_item, current_peer_obj.lamport)
                LOGGER.info(f" After buy call() trader host {trader_host} , trader_port {trader_port}, "
                            f"buyer clock {network_dict[str(current_peer_obj.id)]['lamport']}"
                            f"trader clock {trader_dict['lamport']}")

            except ValueError as e:
                LOGGER.info("No seller found. Please request for another item")
                LOGGER.info(f"Opting new item!")
                network_dict1 = csv_files.csv_ops.get_peers()
                new_item = get_new_item(current_item=current_item)
                LOGGER.info(f"Buyer {current_id} buying new item {new_item}. "
                            f"Old item was {current_item}!")
                network_dict1[str(current_id)]['item'] = new_item
                csv_files.csv_ops.write_peers(network_dict1)

            except Exception as ex:
                LOGGER.exception(f"Failed to execute buy call")

    if current_peer_obj.type == PeerType.SELLER:
        network_dict = csv_files.csv_ops.get_peers()

        LOGGER.info(f"Registering item with the trader {network_dict[str(current_peer_obj.id)]['trader']}")

        network_dict[str(current_peer_obj.id)]['lamport'] += 1

        LOGGER.info(f"Seller clock after this local event:  {network_dict[str(current_peer_obj.id)]['lamport']}")

        trader_id = network_dict[str(current_peer_obj.id)]['trader']
        trader_host = network_dict[str(trader_id)]['_host']
        trader_port = network_dict[str(trader_id)]['_port']

        csv_files.csv_ops.write_peers(network_dict)

        helper = RpcHelper(host=trader_host, port=trader_port)
        trader_connection = helper.get_client_connection()

        LOGGER.info(f"Inside seller call():  {network_dict[str(current_peer_obj.id)]['lamport']}")

        trader_connection.register_products(current_peer_obj.id, network_dict[str(current_peer_obj.id)]['lamport'])


def start_process(current_peer_id: int):
    data = pickle_load_from_file(params_pickle_file_path)

    network_map = data["network_map"]
    item_quantities_map = config.item_quantities_map
    thread_pool_size = config.thread_pool_size

    current_peer_obj: Peer = network_map[current_peer_id]
    ops_obj = OpsFactory.get_ops(network=network_map,
                                 current_peer=current_peer_obj,
                                 item_quantities_map=item_quantities_map,
                                 thread_pool_size=thread_pool_size)

    trigger_thread = Thread(target=handle_process_start, args=(ops_obj, current_peer_obj, network_map))
    trigger_thread.start()

    helper = RpcHelper(host=current_peer_obj.host, port=current_peer_obj.port)

    helper.start_server(ops_obj, current_peer_obj.id)

    LOGGER.info(f"done with processing :{current_peer_obj}")
    ops_obj.shutdown()
