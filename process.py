import random
import time
from threading import Thread
from time import sleep
from typing import Dict

import config
from config import buyer_pool_interval_s, buyer_max_loops, params_pickle_file_path
from enums.peer_type import PeerType
from logger import get_logger
from peer.model import Peer
from rpc.rpc_helper import RpcHelper
from rpc.ops_factory import OpsFactory
from utils import get_new_item, pickle_load_from_file

LOGGER = get_logger(__name__)


def handle_process_start(ops, current_peer_obj: Peer, network_map: Dict[str, Peer]):
    current_id = current_peer_obj.id
    LOGGER.info(f"Start process called for peer {current_id}. Sleeping!")
    sleep(2)

    trader_id = current_peer_obj.trader
    trader_obj = network_map[trader_id]

    if current_peer_obj.type == PeerType.BUYER:
        sleep(5)
        LOGGER.info(f"Initializing buyer flow for peer {current_id}")

        while True:
            # iteration_count = 0
            # item_change_required = False
            current_item = current_peer_obj.item
            LOGGER.info(f"Buyer {current_id} sending buy request for item {current_item}")
            try:
                # TO DO: change the trader after leader election
                current_peer_obj._lamport = current_peer_obj.lamport+1

                helper = RpcHelper(host=trader_obj.host, port=trader_obj.port)
                LOGGER.info(f" trader host {trader_obj.host} , trader_port {trader_obj.port}, "
                            f"buyer clock {current_peer_obj.lamport}"
                            f"trader clock {trader_obj.lamport}")
                trader_connection = helper.get_client_connection()
                trader_connection.buy(current_id, current_item)
                sleep(10)
                LOGGER.info(f" trader host {trader_obj.host} , trader_port {trader_obj.port}, "
                            f"buyer clock {current_peer_obj.lamport}"
                            f"trader clock {trader_obj.lamport}")
                # while True:
                #     sleep(buyer_pool_interval_s)
                #     iteration_count += 1
                #
                #     if iteration_count >= buyer_max_loops:
                #         LOGGER.info(f"Buy request timed out for Buyer {current_id}!")

                # if item_change_required:
                #     LOGGER.info(f"Sleeping for {config.buyer_item_switch_delay_s} seconds before opting new item!")
                #     time.sleep(config.buyer_item_switch_delay_s)
                #
                #     LOGGER.info(f"Opting new item!")
                #
                #     new_item = get_new_item(current_item=current_item)
                #     LOGGER.info(f"Buyer {current_id} buying new item {new_item}. "
                #                 f"Old item was {current_item}!")
                #     ops.item = new_item

            except Exception as ex:
                LOGGER.exception(f"Failed to execute buy call")
    if current_peer_obj.type == PeerType.SELLER:
        LOGGER.info(f"Registering item with the trader {trader_id}")
        helper = RpcHelper(host=trader_obj.host, port=trader_obj.port)
        trader_connection = helper.get_client_connection()
        trader_connection.register_products(current_peer_obj.id)


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

    ops_obj.shutdown()
