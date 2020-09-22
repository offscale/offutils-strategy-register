import json
from sys import version

from offutils.util import iteritems

if version[0] == "2":
    from itertools import imap as map

from inspect import getmembers, isfunction
from operator import itemgetter
from .env import parse_out_env


def parse(config_filename):
    def inner():
        with open(config_filename, "r") as f:
            s = parse_out_env(f.read())
        strategy = json.loads(s.replace("\n", "").encode("string_escape"))

        function_names = tuple(map(itemgetter(0), getmembers(inner, isfunction)))
        return {
            k: (
                lambda f_name: getattr(inner, f_name)(v)
                if f_name in function_names
                else v
            )(to_f_name(k))
            for k, v in iteritems(strategy)
        }

    to_f_name = lambda f: "{f}_parse".format(f=f)

    inner.provider_parse = lambda provider: {
        k: [{key.upper(): val for key, val in iteritems(option)} for option in v]
        for k, v in iteritems(provider)
    }

    return inner()
