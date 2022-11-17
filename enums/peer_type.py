import enum


class PeerType(enum.Enum):
    BUYER = "BUYER"
    SELLER = "SELLER"
    TRADER = "TRADER"


peer_types = [ele for ele in PeerType]
