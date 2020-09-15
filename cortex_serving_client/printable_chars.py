import string
import re


PRINTABLE_CHARS_REGEX = re.compile(f'[^{re.escape(string.printable)}]')


def remove_non_printable(s: str) -> str:
    return PRINTABLE_CHARS_REGEX.sub('', s)
