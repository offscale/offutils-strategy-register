from unittest import TestCase, main as unittest_main
from os import environ

from offutils_strategy_register.parser.env import parse_out_env


class TestParseEnv(TestCase):
    def test_env(self):
        environ['bar'] = 'foo'
        input_strings = (
            'foo bar "env.bar" can haz', "env.bar",
            '"env.bar"', "'env.bar'",
            "'env.bar'}", '"env.bar"}', '', 'env.'
        )
        map(lambda input_s: self.assertEqual(parse_out_env(input_s),
                                             input_s.replace('env.bar', environ['bar'])),
            input_strings)

    def test_env_edge_case(self):
        environ['bar'] = 'foo'
        self.assertEqual(*(lambda s: (parse_out_env(s), s.replace('env.bar', environ['bar'])))("'env.bar'}"))


if __name__ == '__main__':
    unittest_main()
