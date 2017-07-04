from __future__ import absolute_import

import json
import pytest
from datacloud.common.google import read_sheet


@pytest.mark.parametrize("sheet_name,column_id", [
    ('DnB Attributes', 'Internal Name'),
    ('Existing Attributes', 'ExternalColumnID'),
    ('Derived Attributes', 'ExternalColumnID'),
])
def test_attrs(sheet_name, column_id):
    data = read_sheet(sheet_name)
    for item in data:
        assert_valid_am_attr(item, column_id)

def assert_valid_am_attr(item, column_id):
    if item['Add To New Account Master'] == 'Y':
        assert_item_has_attr(item, column_id)
        assert_item_has_attr(item, 'Category')
        assert_item_has_attr(item, 'Display Name')

        if item['Model Tag'] == 'Y':
            assert_item_has_attr(item, 'StatisticalType')

        if item['Insights Tag'] == 'Y':
            assert_item_has_attr(item, 'FundamentalType')

def assert_item_has_attr(item, attr):
    assert attr in item and item[attr].strip() != '', \
        "Attribute [%s] has empty value for [%s]" % (json.dumps(item), attr)