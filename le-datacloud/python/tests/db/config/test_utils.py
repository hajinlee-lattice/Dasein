# -*- coding: UTF-8 -*-
from __future__ import absolute_import

import pytest
from datacloud.db.config.utils import utf8_to_latin1


@pytest.mark.parametrize("latin1, utf8", [
    (u"Landlr – The All", u"Landlr - The All"),
    (u"based infrastructures—all", u"based infrastructures-all"),
    (u"Angie’s List", u"Angie's List"),
    (u"Netscape⌐", u"Netscape")
])
def test_utf8_to_latin1(latin1, utf8):
    assert utf8_to_latin1(latin1) == utf8
