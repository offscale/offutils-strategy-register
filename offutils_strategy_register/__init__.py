#!/usr/bin/env python

from __future__ import print_function

from collections import namedtuple
from operator import itemgetter

from offutils.util import iteritems

try:
    import cPickle as pickle
except ImportError:
    import pickle

from sys import version

if version[0] == "2":
    from codecs import encode
    from itertools import imap as map
else:
    from codecs import encode as _encode

    def identity(*args):
        return args[0] if len(args) == 1 else args

    def encode(s):
        return s if isinstance(s, bytes) else _encode(s)


from codecs import getreader

import etcd3
from libcloud.compute.base import Node
from libcloud.compute.providers import get_driver
from libcloud.compute.types import NodeState
from offutils import update_d

# TODO: Automatically import the right ones rather than listing:


__author__ = "Samuel Marks"
__version__ = "0.0.10"

# Namedtuples
MarshallLoads = namedtuple("MarshallLoads", "loads")
KeyVal = namedtuple("KeyVal", "key value")

# Types which can be easily serialised
normal_types = (
    type(dict),
    type(list),
    type(tuple),
    type(bool),
    type(float),
    type(str),
    type(int),
    type(None),
)

_get_client = etcd3.client

reader = getreader("utf-8")

save_node_info = lambda node_name, node_info, folder="unclustered", marshall=pickle, **client_kwargs: _get_client(
    **client_kwargs
).put(
    "/".join((folder, node_name)), encode(marshall.dumps(node_info))
)

del_node_info = lambda node_name, folder="unclustered", marshall=pickle, **client_kwargs: _get_client(
    **client_kwargs
).delete(
    "/".join((folder, node_name))
)

if version[0] == "3":
    from operator import methodcaller

    str_dammit = methodcaller("decode", encoding="utf8")
else:

    def identity(*args):
        return args[0] if len(args) == 1 else args

    str_dammit = identity


old = b"\x80\x04\x95\x01\x03\x00\x00\x00\x00\x00\x00\x8c\x15libcloud.compute.base\x94\x8c\x04Node\x94\x93\x94)\x81\x94}\x94(\x8c\x02id\x94\x8c\x013\x94\x8c\x04name\x94\x8c\x07dummy-3\x94\x8c\x05state\x94\x8c\x16libcloud.compute.types\x94\x8c\tNodeState\x94\x93\x94\x8c\x07running\x94\x85\x94R\x94\x8c\npublic_ips\x94]\x94\x8c\t127.0.0.3\x94a\x8c\x0bprivate_ips\x94]\x94\x8c\x06driver\x94\x8c\x1elibcloud.compute.drivers.dummy\x94\x8c\x0fDummyNodeDriver\x94\x93\x94)\x81\x94}\x94(\x8c\x05creds\x94\x8c\x00\x94\x8c\x02nl\x94]\x94(h\x02)\x81\x94}\x94(h\x05\x8c\x011\x94h\x07\x8c\x07dummy-1\x94h\th\x0fh\x10]\x94\x8c\t127.0.0.1\x94ah\x13]\x94h\x15h\x19\x8c\x04size\x94N\x8c\ncreated_at\x94N\x8c\x05image\x94N\x8c\x05extra\x94}\x94\x8c\x03foo\x94\x8c\x03bar\x94s\x8c\x05_uuid\x94Nubh\x02)\x81\x94}\x94(h\x05\x8c\x012\x94h\x07\x8c\x07dummy-2\x94h\th\x0fh\x10]\x94h$ah\x13]\x94h\x15h\x19h&Nh'Nh(Nh)}\x94h+h,sh-Nubh\x03e\x8c\nconnection\x94h\x16\x8c\x0fDummyConnection\x94\x93\x94)\x81\x94}\x94(\x8c\x06secure\x94K\x01\x8c\x02ua\x94]\x94\x8c\x07context\x94}\x94\x8c\x0crequest_path\x94h\x1c\x8c\x04port\x94M\xbb\x01\x8c\x07timeout\x94N\x8c\x0bretry_delay\x94N\x8c\x07backoff\x94N\x8c\tproxy_url\x94N\x8c\x03key\x94h\x1cububh&h\x00\x8c\x08NodeSize\x94\x93\x94)\x81\x94}\x94(h\x05\x8c\x02s1\x94h\x07h+\x8c\x03ram\x94M\x00\x08\x8c\x04disk\x94K\xa0\x8c\tbandwidth\x94N\x8c\x05price\x94G\x00\x00\x00\x00\x00\x00\x00\x00h\x15h\x19h)}\x94h-Nubh'Nh(h\x00\x8c\tNodeImage\x94\x93\x94)\x81\x94}\x94(h\x05\x8c\x02i2\x94h\x07h(h\x15h\x19h)}\x94h-Nubh)}\x94h+h,sh-Nub."


get_node_info = lambda node_name, folder="unclustered", marshall=pickle, **client_kwargs: marshall.loads(
    _get_client(**client_kwargs).get("/".join((folder, node_name)))[0]
)

obj_to_d = (
    lambda obj: obj
    if isinstance(obj, dict)
    else {k: getattr(obj, k) for k in dir(obj) if not k.startswith("_")}
)


def node_to_dict(node):
    node_d = {
        attr: getattr(node, attr)
        for attr in dir(node)
        if not attr.startswith("__")
        and type(getattr(node, attr)) in normal_types
        and getattr(node, attr)
        or attr in frozenset(("id", "name", "state", "public_ips", "private_ips"))
    }
    node_d["driver"] = (
        node.driver.__name__
        if node.driver.__class__.__name__ == "type"
        else node.driver.__class__.__name__
    )

    if hasattr(node, "extra") and node.extra:
        if (
            "network_interfaces" in node_d.get("extra", {})
            and node_d["extra"]["network_interfaces"]
        ):
            node_d["extra"]["network_interfaces"] = [
                interface
                if isinstance(interface, dict)
                else {"name": interface.name, "id": interface.id}
                for interface in node_d["extra"]["network_interfaces"]
            ]
        node_d["extra"] = {
            k: v
            for k, v in iteritems(node.extra)
            if k not in ("secret", "key") and type(v) in normal_types
        }
    if hasattr(node, "availability_zone"):
        node_d["availability_zone"] = obj_to_d(node.availability_zone)

    if "connectionCls" in node_d:
        del node_d["connectionCls"]
    return node_d


def dict_to_node(d):
    assert isinstance(d, dict)

    if "driver_cls" not in d:
        print('d["extra"]:', d["extra"], ";")
        d["driver_cls"] = get_driver(d["extra"]["provider"])
        d["_class"] = d["driver_cls"].__name__

    if "state" in d and isinstance(d["state"], (str, bytes)):
        d["state"] = getattr(NodeState, d["state"].upper())

    _class = d.pop("_class")
    driver_cls = d.pop("driver_cls")
    for key in "get_uuid", "uuid":
        d.pop(key, None)
    # d['driver'] = driver_cls

    try:
        return globals()[_class](**d)
    except (TypeError, KeyError):
        return Node(**(update_d(d, driver=driver_cls)))


def dict_to_cls(d):
    assert isinstance(d, dict)
    _class = d.pop("_class")
    # if _class not in globals(): return import_module(_class)(**d)
    return _class(**d)


def print_dict_and_type(d):
    for key, val in iteritems(d):
        print("key = '{0}', val = `{1}`, type = {2}".format(key, val, type(val)))


def list_nodes(
    folder="unclustered", marshall=MarshallLoads(lambda s: s), **client_kwargs
):
    return list(_fetch_nodes(folder, marshall=marshall, **client_kwargs))


def fetch_node(
    folder="unclustered", marshall=MarshallLoads(lambda s: s), **client_kwargs
):
    return next(_fetch_nodes(folder, marshall=marshall, **client_kwargs))


def _fetch_nodes(
    folder="unclustered", marshall=MarshallLoads(lambda s: s), **client_kwargs
):
    _client = _get_client(**client_kwargs)
    return map(
        lambda _node: KeyVal("/".join((folder, _node.name)), _node),
        map(
            dict_to_node,
            filter(
                lambda _node: "public_ips" in _node,
                (
                    map(
                        marshall.loads,
                        map(
                            itemgetter(0),
                            _client.get_prefix(
                                folder[1:] if folder.startswith("/") else folder
                            ),
                        ),
                    )
                ),
            ),
        ),
    )


__all__ = [
    "normal_types",
    "save_node_info",
    "get_node_info",
    "del_node_info",
    "list_nodes",
    "fetch_node",
    "node_to_dict",
    "dict_to_node",
    "dict_to_cls",
    "obj_to_d",
]
