from __future__ import print_function

import random

from . import matchkeys
from .constants import DOMAIN, DUNS, MATCH_KEY_COLS, NAME


def random_from_records(records, col_generator, weights=None):
    """Randomly choose a record from input list (with probability based on respective weight).
    Only columns specified with input generator function will be returned, other columns are
    filled with None.

    Arguments:
        records {list(list(str))} -- list of records (list of column values)
        col_generator -- generator function (or normal function) that generate/return an iterable of
                         column names

    Keyword Arguments:
        weights {list(float)} -- weights of corresponding record (default: {None})
    """
    if weights is not None:
        assert len(records) == len(weights), "Length of records and weights should be the same"

    while True:
        record = None
        cols = get_cols(col_generator)
        if weights is None:
            # all with the same weight by default
            record = random.choice(records)
        else:
            record = weighted_choice(records, weights)
        res = []
        for idx, col in enumerate(MATCH_KEY_COLS):
            if col in cols:
                res.append(record[idx])
            else:
                res.append(None)
        yield res


def random_from_columns(value_map, weight_map=None):
    """Generate a record that contains all column in input value map. Each column
       value of the record is randomly chosen from an input list of possible values.

    Arguments:
        value_map {dict(str, list(str))} -- key of the dict is column name, value is
                                            a list of possible column value

    Keyword Arguments:
        weight_map {dict(str, float)} -- key is column name, value is a list of corresponding weights. (default: {None})
    """
    while True:
        res = []
        for col in MATCH_KEY_COLS:
            if col in value_map:
                if weight_map is None:
                    # all with the same weight by default
                    res.append(random.choice(value_map[col]))
                else:
                    # make sure value list & weight list have the same length
                    assert len(value_map[col]) == len(weight_map[col])
                    res.append(weighted_choice(value_map[col], weight_map[col]))
            else:
                res.append(None)
        yield res

def dominant_weight_map_from_value_map(value_map, dominant_col, probability):
    # Generate weight map for all columns in value_map. The weight for FIRST value of dominant_col
    # will be the same as input probability and the rest values will share (1 - probability).
    # The other columns will have evenly distributed weights.
    assert probability >= 0.0 and probability <= 1.0
    assert dominant_col in value_map
    weight_map = dict()
    for col in value_map:
        total_weight, size = 1.0, len(value_map[col])
        weight_map[col] = []
        if col == dominant_col:
            weight_map[col].append(probability)
            total_weight = total_weight - probability
            size = size - 1
        weight_map[col] = weight_map[col] + [ total_weight / size for i in range(0, size) ]
    return weight_map

def value_map_from_group(grp_idx, cols, num_values):
    # Generate value map with fake data. All columns specified in input will be returned and
    # the number of possible values will be the same as "num_values"
    value_map = dict()
    for col in cols:
        values = []
        for i in range(0, num_values):
            if col == DUNS:
                values.append(next_duns(grp_idx, i))
            elif col == DOMAIN:
                values.append(next_domain(grp_idx, i))
            else:
                values.append('GRP{grp_idx}_{col}_{idx}'.format(
                    grp_idx=grp_idx, col=col, idx=i))
        value_map[col] = values
    return value_map

def from_group(grp_idx, col_generator, counters):
    """Generate a record that contains all columns provided by input column generator.
    Column value of that record is generated using given test group index and a counter that
    counts how many values of that column is already generated.

    Arguments:
        grp_idx {int} -- test group index
        col_generator -- generator function (or normal function) that generate/return an iterable of
                         column names
        counters {dict(string, int)} -- key = column name, value = number of values generated (for this column)
    """
    while True:
        res = []
        cols = get_cols(col_generator)
        for col in MATCH_KEY_COLS:
            if col in cols:
                if col == DUNS:
                    res.append(next_duns(grp_idx, counters[col]))
                elif col == DOMAIN:
                    res.append(next_domain(grp_idx, counters[col]))
                else:
                    res.append('GRP{grp_idx}_{col}_{idx}'.format(
                        grp_idx=grp_idx, col=col, idx=counters[col]))
                counters[col] += 1
            else:
                res.append(None)
        yield res

def random_retain(cols, fixed_cols=None):
    # Random one combaintion from a set of columns. Any column in fixed_cols will
    # guarantee to be in the return set.
    # TODO probably has library
    # TODO probably has a better name for this function
    while True:
        res = set()
        for col in cols:
            if fixed_cols is not None and col in fixed_cols:
                res.add(col)
            elif random.random() < 0.5:
                res.add(col)
        yield res

def random_combination(cols, m, post_process_fn=None):
    # Generator function for matchkeys.random_combination
    # post process the generated columns if necessary
    while True:
        m_cols = matchkeys.random_combination(cols, m)
        if post_process_fn is not None:
            post_process_fn(m_cols)
        yield m_cols


def next_duns(grp_idx, idx):
    """Generate fake DUNS with input test group index and test record index

    Arguments:
        grp_idx {int} -- test group index (should be < 10**4)
        idx {int} -- test record index (within group, should be < 10**5)

    Returns:
        str -- generated DUNS
    """
    return '{:04d}{:05d}'.format(grp_idx, idx)


def next_domain(grp_idx, idx):
    """Generate fake domain with input test group index and test record index

    Arguments:
        grp_idx {int} -- test group index
        idx {int} -- test record index (within group)

    Returns:
        str -- generated domain
    """
    return 'grp-{grp_idx}-{idx}.com'.format(grp_idx=grp_idx, idx=idx)


def get_cols(col_generator):
    try:
        return next(col_generator)
    except:
        return col_generator()


def weighted_choice(choices, weights):
    """Choose one item from a list based on given weight

    Arguments:
        choices {list(any)} -- list of items to choose from
        weights {list(float)} -- list of corresponding weights for each item

    Returns:
        any -- chosen item from the list
    """
    total = sum(w for w in weights)
    r = random.uniform(0, total)
    upto = 0
    for i, c in enumerate(choices):
        w = weights[i]
        if upto + w >= r:
            return c
        upto += w
    assert False, "Shouldn't get here"
