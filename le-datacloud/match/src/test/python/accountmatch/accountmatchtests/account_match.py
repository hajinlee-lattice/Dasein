"""
For File-based Account Match Test Data Preparation

Refer to confluence page:
https://confluence.lattice-engines.com/display/ENG/M26+Account+Match+Tests
"""

from random import sample
from collections import defaultdict
from typing import List
import lib.generators as generators
import lib.constants as constants
import testgroup.noconflict as noconflict
import testgroup.conflict as conflict
import testgroup.randomvalue as randomvalue


grp_idx = 0


def get_n_incr():
    global grp_idx
    idx = grp_idx
    grp_idx += 1
    return idx


def generate_controlled_group_1() -> List[noconflict.Group]:
    """
    Generate test data for controlled group 1.\n
    Scenarios are:
        1) all match keys
        2) system Ids only
        3) non-system Ids only
        4) 2 system Ids + Name, Country, DUNS
    Data size: 10, 50, 100 records for each scenario.\n
    :return: List of no-conflict test groups.
    """

    account_id_template = 'account{}'
    saleforce_id_template = 'sfdc{}'
    marketo_id_template = 'mkto{}'
    domain_name_duns_locations = [{
        'domain': 'google.com',
        'name': 'google',
        'duns': '999999999',
        'country': 'usa',
        'state': 'ca',
        'city': 'mountain view'
    }, {
        'domain': 'amazon.com',
        'name': 'amazon',
        'duns': '888888888',
        'country': 'usa',
        'state': 'wa',
        'city': 'seattle'
    }, {
        'domain': 'uber.com',
        'name': 'uber',
        'duns': '777777777',
        'country': 'usa',
        'state': 'ca',
        'city': 'san francisco'
    }]
    num_records = [10, 50, 100]
    result = []
    for scenario in noconflict.Group.SCENARIOS:
        if scenario == noconflict.Group.ALL_MATCH_KEYS or scenario == noconflict.Group.MIX:
            populate_universe = True
        else:
            populate_universe = False
        for i in range(len(num_records)):
            num = num_records[i]
            # print('Controlled group 1: scenario={}, num_records={}, populate_universe={}'
            #       .format(scenario, num, populate_universe))
            record = [account_id_template.format(i), saleforce_id_template.format(i),
                      marketo_id_template.format(i), domain_name_duns_locations[i]['name'],
                      domain_name_duns_locations[i]['domain'], domain_name_duns_locations[i]['duns'],
                      domain_name_duns_locations[i]['country'], domain_name_duns_locations[i]['state'],
                      domain_name_duns_locations[i]['city']] \
                if scenario == noconflict.Group.ALL_MATCH_KEYS else None
            result.append(noconflict.Group(get_n_incr(), num, scenario=noconflict.Group.ALL_MATCH_KEYS,
                                           populate_universe=populate_universe, record=record))
    print('Controlled group 1: generated {} groups.\n'.format(len(result)))
    return result


# (high_priority_match_key, low_priority_match_key)
match_key_combinations = [
    (set(constants.SYS_ID_COLS), set(constants.SYS_ID_COLS)),
    (set(constants.SYS_ID_COLS), constants.DUNS),
    (set(constants.SYS_ID_COLS), {constants.NAME, constants.COUNTRY}),
    (constants.DUNS, {constants.NAME, constants.COUNTRY}),
    ({constants.DOMAIN, constants.COUNTRY}, constants.DUNS),
    ({constants.DOMAIN, constants.COUNTRY}, {constants.NAME, constants.COUNTRY})]
num_values = [2, 5, 10, 20]
record_ratios = [2, 5, 10]


def _next_value(field_name: str, group_index: int, record_index: int) -> str:
    if field_name == constants.ACCOUNT_ID:
        value = 'ACCT{}'.format(group_index)
    elif field_name == constants.SFDC_ID:
        value = 'SFDC{}'.format(group_index)
    elif field_name == constants.MKTO_ID:
        value = 'MKTO{}'.format(group_index)
    elif field_name == constants.DUNS:
        value = generators.next_duns(group_index, record_index)
    elif field_name == constants.DOMAIN:
        value = generators.next_domain(group_index, record_index)
    elif field_name == constants.COUNTRY:
        value = 'usa'
    elif field_name == constants.NAME:
        value = 'name-{}-{}'.format(group_index, record_index)
    else:
        print('WARNING: unknown field_name=' + str(field_name))
        value = 'unknown'
    return value


def generate_controlled_group_2() -> List[conflict.Group]:
    """
    Generate test data for controlled group 2.\n
    Different values for lower priority match keys.
    :return: List of conflict test groups.
    """

    result = []
    for hi, lo in match_key_combinations:
        for num_diff in num_values:
            for record_ratio in record_ratios:
                grp_idx = get_n_incr()
                primary_key = sample(hi, 1)[0] if type(hi) == set else hi
                primary_val = _next_value(primary_key, grp_idx, 1234)
                secondary_keys = list(lo) if type(lo) == set else [lo]
                fn = generators.from_group(grp_idx, lambda: [primary_key] + secondary_keys, defaultdict(int))
                result.append(conflict.Group(grp_idx, num_diff * record_ratio,
                                             (primary_key, primary_val), set(secondary_keys), fn))
    print('Controlled group 2: generated {} groups.\n'.format(len(result)))
    return result


def generate_controlled_group_3() -> List[conflict.Group]:
    """
    Generate test data for controlled group 3.\n
    Different values for high priority match keys.
    :return: List of conflict test groups.
    """

    result = []
    for hi, lo in match_key_combinations:
        for num_diff in num_values:
            for record_ratio in record_ratios:
                grp_idx = get_n_incr()
                primary_key = sample(lo, 1)[0] if type(lo) == set else lo
                primary_val = _next_value(primary_key, grp_idx, 1234)
                secondary_keys = list(hi) if type(hi) == set else [hi]
                fn = generators.from_group(grp_idx, lambda: [primary_key] + secondary_keys, defaultdict(int))
                result.append(conflict.Group(grp_idx, num_diff * record_ratio,
                                             (primary_key, primary_val), set(secondary_keys), fn))
    print('Controlled group 3: generated {} groups.\n'.format(len(result)))
    return result


def generate_random_group_1() -> List[randomvalue.Group]:
    scenarios = [
        constants.MATCH_KEY_COLS,
        constants.SYS_ID_COLS,
        # non-system Ids
        [constants.NAME, constants.DOMAIN, constants.DUNS, constants.COUNTRY, constants.STATE, constants.CITY]
    ]
    num_values = [10, 50, 100]
    num_records = [50, 100, 500, 1000]
    result = []
    for scenario in scenarios:
        for num_value in num_values:
            for num in num_records:
                grp_idx = get_n_incr()
                col_fn = generators.random_retain(scenario)
                value_map = generators.value_map_from_group(grp_idx, scenario, num_value)
                value_fn = generators.random_from_columns(value_map)
                result.append(randomvalue.Group(grp_idx, num, col_fn, value_fn,
                                                enforce_match_key_combination=True))
    print('Random group 1: generated {} groups.\n'.format(len(result)))
    return result


def generate_random_group_2() -> List[randomvalue.Group]:
    scenario = constants.MATCH_KEY_COLS
    num_values = [10, 50, 100]
    record_ratios = [2, 5, 10]
    result = []
    for num_value in num_values:
        for ratio in record_ratios:
            grp_idx = get_n_incr()
            col_fn = generators.random_retain(scenario)
            value_map = generators.value_map_from_group(grp_idx, scenario, num_value)
            value_fn = generators.random_from_columns(value_map)
            result.append(randomvalue.Group(grp_idx, num_value * ratio, col_fn, value_fn,
                                            populate_probability=0.4))
    print('Random group 2: generated {} groups.\n'.format(len(result)))
    return result


def _get_dominant_values(dominant_field: str, num_values: int, group_index: int) -> List[str]:
    if dominant_field == constants.DOMAIN:
        result = ['netflix.com', 'yahoo.com'] + ['domain{}.com'.format(i) for i in range(num_values)]
    elif dominant_field == constants.COUNTRY:
        result = ['usa', 'china', 'japan'] + ['country{}'.format(i) for i in range(num_values)]
    elif dominant_field == constants.DUNS:
        result = ['123456789', '987654321'] + [''.join(sample(str(num_values), 1)[0] * 9)
                                               for _ in range(num_values)]
    elif dominant_field == constants.ACCOUNT_ID:
        result = ['AccountId{}'.format(group_index),
                  'AccountId{}{}'.format(group_index, num_values)] \
                 + ['OtherAccounts{}'.format(i) for i in range(num_values)]
    elif dominant_field == constants.SFDC_ID:
        result = ['SalesforceId{}'.format(group_index),
                  'SaleforceId{}{}'.format(group_index, num_values)] \
                 + ['OtherSalesforceId{}'.format(i) for i in range(num_values)]
    elif dominant_field == constants.MKTO_ID:
        result = ['MarketoId{}'.format(group_index),
                  'MarketoId{}{}'.format(group_index, num_values)] \
                 + ['OtherMarketoId{}'.format(i) for i in range(num_values)]
    else:
        print('WARNING: unknown field_name=' + dominant_field)
        result = 'unknown'
    return result


def generate_random_group_3() -> List[randomvalue.Group]:
    dominant_fields = [
        {constants.DOMAIN, constants.COUNTRY},
        constants.DUNS,
        constants.SYS_ID_COLS
    ]
    dominant_ratios = [0.5, 0.8, 1.0]
    num_records = [50, 100, 50, 1000]
    result = []
    for dominant_field in dominant_fields:
        field = sample(dominant_field, 1)[0] \
            if (type(dominant_field) == set or type(dominant_field) == list) else dominant_field
        for ratio in dominant_ratios:
            for num in num_records:
                grp_idx = get_n_incr()
                values = _get_dominant_values(field, num_values=10, group_index=grp_idx)
                col_fn = generators.random_retain(constants.MATCH_KEY_COLS, fixed_cols=[field])
                value_map = generators.value_map_from_group(grp_idx, constants.MATCH_KEY_COLS, 10)
                value_map[field] = values
                weight_map = generators.dominant_weight_map_from_value_map(value_map, field, ratio)
                value_fn = generators.random_from_columns(value_map, weight_map=weight_map)
                result.append(randomvalue.Group(grp_idx, num, col_fn, value_fn))
    print('Random group 3: generated {} groups.\n'.format(len(result)))
    return result


def get_test_groups():
    return generate_controlled_group_1() + generate_controlled_group_2() + generate_controlled_group_3() \
           + generate_random_group_1() + generate_random_group_2() + generate_random_group_3()
