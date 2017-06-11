from __future__ import absolute_import
import pytest

from datacloud.common.cipher import encrypt, decrypt

def test_codec():
    enc = encrypt('welcome')
    dec = decrypt(enc)
    assert dec == 'welcome'

def test_end_with_space():
    with pytest.raises(Exception):
        encrypt('welcome ')