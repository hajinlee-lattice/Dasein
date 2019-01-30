import random

import constants

def random_combination(cols, m):
    """Pick m columns from the given list. After that, fill missing columns to make sure
    match key combinations are valid (e.g., must have name/country if state is provided).

    Arguments:
        cols {list(str)} -- list of column names to choose from
        m {int} -- number of columns in the random combination

    Returns:
        set(str) -- set of randomly generated columns (will have length >= m)
    """
    pool = tuple(cols)
    n = len(pool)
    indices = sorted(random.sample(xrange(n), m))
    return fill_missing_col(set(pool[i] for i in indices))

def fill_missing_col(cols):
    # Make sure match key combinations are valid. Not filling any column when there is country only
    # because not sure which one to fill (domain/name)
    if constants.STATE in cols:
        cols.add(constants.COUNTRY)
        cols.add(constants.NAME)
    if constants.CITY in cols:
        cols.add(constants.STATE)
        cols.add(constants.COUNTRY)
        cols.add(constants.NAME)
    if constants.DOMAIN in cols or constants.NAME in cols:
        # just in case, probably not needed as actors should fill this field
        cols.add(constants.COUNTRY)
    return cols

if __name__ == '__main__':
    res = random_combination(constants.MATCH_KEY_COLS, 3)
    print res
