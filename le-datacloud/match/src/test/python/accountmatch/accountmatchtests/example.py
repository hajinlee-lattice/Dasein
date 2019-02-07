from collections import defaultdict

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

def group1():
    # example for controlled group 1-a
    # 1. test data will use the given record
    # 2. no data in universe before testing
    record = [ 'account1', 'sfdc1', 'mkto1', 'google', 'google.com', '999999999', 'usa', 'ca', 'mountain view' ]
    return noconflict.Group(get_n_incr(), 50, scenario=noconflict.Group.ALL_MATCH_KEYS, populate_universe=False, record=record)

def group2():
    # example for controlled group 1
    # 1. generate fake test data automatically since record is not given
    # 2. one record in universe before testing
    return noconflict.Group(get_n_incr(), 100, scenario=noconflict.Group.SYS_ID_ONLY, populate_universe=True)

def group3():
    # example for controlled group 2
    # 1. highest priority key = AccountID, value = A003
    # 2. lower priority key = DUNS, values = generated with group index
    # 3. no data in universe before testing
    grp_idx = get_n_incr()
    fn = generators.from_group(
        grp_idx, lambda: [ constants.ACCOUNT_ID, constants.DUNS ], defaultdict(int))
    return conflict.Group(grp_idx, 5, (constants.ACCOUNT_ID, 'A003'), set([constants.DUNS]), fn)

def group4():
    # example for controlled group 3
    # Note that this is similar to previous example. However, key with common value is now DUNS,
    # which has lower priority than AccountId
    grp_idx = get_n_incr()
    fn = generators.from_group(
        grp_idx, lambda: [ constants.ACCOUNT_ID, constants.DUNS ], defaultdict(int))
    common_duns = generators.next_duns(grp_idx, 1234) # prevent using the same value as other groups
    return conflict.Group(grp_idx, 5, (constants.DUNS, common_duns), set([constants.ACCOUNT_ID]), fn)

def group5():
    # example for random group 1-a
    # 1. all match keys are used
    # 2. 10 values for each column
    # 3. for random group 1, need to make sure match keys are valid
    grp_idx = get_n_incr()
    # randomly generate column combination
    col_fn = generators.random_retain(constants.MATCH_KEY_COLS)
    # each column has 10 possible values
    value_map = generators.value_map_from_group(grp_idx, constants.MATCH_KEY_COLS, 10)
    value_fn = generators.random_from_columns(value_map)
    return randomvalue.Group(grp_idx, 100, col_fn, value_fn, enforce_match_key_combination=True)

def group6():
    # example for random group 2
    # Note that this is similar to the previous example. Only difference between random group 1 & 2
    # is that group 2 can have invalid match key combination
    # TODO make sure this group is required (not sure if it passes validation for bulk match input)
    grp_idx = get_n_incr()
    col_fn = generators.random_retain(constants.MATCH_KEY_COLS)
    value_map = generators.value_map_from_group(grp_idx, constants.MATCH_KEY_COLS, 10)
    value_fn = generators.random_from_columns(value_map)
    # only difference here, not enforcing combination
    # also put 40% of data (technically 40% to put each record) into universe before testing
    return randomvalue.Group(grp_idx, 100, col_fn, value_fn, populate_probability=0.4)

def group7():
    # example for random group 3-b
    # 1. Dominant Domain (50% chance to choose one value) (e.g., a lot of netflix.com)
    # 2. Use all match keys, other columns has 10 possible values

    grp_idx = get_n_incr()
    col_fn = generators.random_retain(constants.MATCH_KEY_COLS, fixed_cols=[constants.DOMAIN])
    # use fake data as an example, can also provide pre-defined records
    value_map = generators.value_map_from_group(grp_idx, constants.MATCH_KEY_COLS, 10)
    # can override values for domain to use real values (BE CAREFUL not to use domain from other groups)
    value_map[constants.DOMAIN] = [ 'netflix.com', 'yahoo.com' ] + [ 'domain{}.com'.format(i) for i in range(0, 8) ]
    # first domain (netflix.com) will have 50% of being chosen, the others will share the remaining 50% probability
    # weights will be [ 0.5, ~0.55, 0.55, ... ]
    weight_map = generators.dominant_weight_map_from_value_map(value_map, constants.DOMAIN, 0.5)
    # specify weights so that the first DUNS will be picked more often
    value_fn = generators.random_from_columns(value_map, weight_map=weight_map)
    return randomvalue.Group(grp_idx, 200, col_fn, value_fn)

def get_test_groups():
    # NOTE any test file need to define this function (used by main script to retrieve a list of defined test groups)
    return [ group1(), group2(), group3(), group4(), group5(), group6(), group7() ]
