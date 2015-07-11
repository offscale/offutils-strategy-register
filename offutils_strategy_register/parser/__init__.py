import json

from inspect import getmembers, isfunction
from operator import itemgetter
from env import parse_out_env


def parse(config_filename):
    def inner():
        with open(config_filename, 'r') as f:
            s = parse_out_env(f.read())
        strategy = json.loads(s.replace('\n', '').encode('string_escape'))

        function_names = map(itemgetter(0), getmembers(inner, isfunction))
        return {k: (lambda f_name: getattr(inner, f_name)(v) if f_name in function_names else v)(to_f_name(k))
                for k, v in strategy.iteritems()}

    to_f_name = lambda f: '{f}_parse'.format(f=f)

    inner.provider_parse = lambda provider: {
        k: map(lambda option: {key.upper(): val for key, val in option.iteritems()}, v)
        for k, v in provider.iteritems()
    }

    return inner()
