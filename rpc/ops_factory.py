from typing import Dict

from enums.peer_type import PeerType
from peer.model import Peer
from rpc.buyer_ops import BuyerOps
from rpc.seller_ops import SellerOps


class OpsFactory:

    @staticmethod
    def get_ops(network: Dict[str, Peer],
                current_peer: Peer,
                item_quantities_map: dict,
                thread_pool_size=20,
                ):
        peer_type = current_peer.type
        if peer_type == PeerType.SELLER:
            return SellerOps(network,
                             current_peer,
                             item_quantities_map,
                             thread_pool_size)

        if peer_type == PeerType.BUYER:
            return BuyerOps(network,
                            current_peer,
                            thread_pool_size)
