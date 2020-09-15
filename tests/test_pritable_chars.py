import unittest

from cortex_serving_client.printable_chars import remove_non_printable


class PrintableCharsTest(unittest.TestCase):

    def test_remove_non_printable(self):
        self.assertEqual('test test  text text', remove_non_printable('test test \b text \btext'))
