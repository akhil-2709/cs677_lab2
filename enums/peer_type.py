import enum


class PeerType(str,enum.Enum):
    BUYER = "BUYER"
    SELLER = "SELLER"
    TRADER = "TRADER"


peer_types = [ele for ele in PeerType]
