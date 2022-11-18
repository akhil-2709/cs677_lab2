from typing import Dict

from enums.peer_type import PeerType
from peer.model import Peer
from rpc.ops import CommonOps
from trader_list import TraderList


class OpsFactory:

    @staticmethod
    def get_ops(network: Dict[str, Peer],
                current_peer: Peer,
                item_quantities_map: dict,
                thread_pool_size=20,
                ):
        peer_type = current_peer.type
        trader_obj = TraderList(network)
        return CommonOps(network,
                         current_peer,
                         item_quantities_map,
                         trader_obj,
                         thread_pool_size
                         )
