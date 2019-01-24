import math
import random
from collections import defaultdict

import lib.generators as generators
import lib.matchkeys as matchkeys
from lib.constants import (ACCOUNT_ID, DOMAIN, DUNS, MATCH_KEY_COLS, MKTO_ID,
                           NAME, SFDC_ID, SYS_ID_COLS)


def more_than_half(cnt):
    # return the smallest integer that > (cnt / 2)
    return int(math.ceil((cnt + 1) / 2))

class Group(object):
    TYPE = 'controlled_no_conflict'
    # test scenarios
    ALL_MATCH_KEYS = set([ ACCOUNT_ID, SFDC_ID, MKTO_ID, NAME, DOMAIN, DUNS ])
    SYS_ID_ONLY = set(SYS_ID_COLS)
    NO_SYS_ID = set([ NAME, DOMAIN, DUNS ])
    MIX = set([ ACCOUNT_ID, SFDC_ID, NAME, DUNS ])
    SCENARIOS = [ ALL_MATCH_KEYS, SYS_ID_ONLY, NO_SYS_ID, MIX ]

    def __init__(self, grp_idx, num_records, scenario=ALL_MATCH_KEYS, populate_universe=False, record=None):
        # 1. Generate num_records with column values from a single record (use the record variable or
        #    generate with group index if record is None).
        # 2. Scenario represents which columns are generated
        # 3. If populate_universe is True, one of the generated records will be put into pre test data.
        self._scenario = set(scenario)
        self._grp_idx = grp_idx
        self._num_records = num_records
        self._populate_universe = populate_universe
        self._record = record
        self._test_data = []
        self._pre_test_data = []
        if self._record is None:
            # use default
            row_generator = generators.from_group(grp_idx, lambda: MATCH_KEY_COLS, defaultdict(int))
            self._record = next(row_generator)
        self._generated = False
        self._check_scenario()

    def get_pre_test_data(self):
        self._generate_data()
        return self._pre_test_data

    def get_test_data(self):
        self._generate_data()
        return self._test_data

    def _generate_data(self):
        if self._generated:
            return

        self._generated = True

        col_generator = generators.random_combination(
            list(self._scenario), more_than_half(len(self._scenario)), lambda c: self._fill(c))
        data_generator = generators.random_from_records([ self._record ], col_generator)
        for i in range(0, self._num_records):
            # defensive copy
            row = list(next(data_generator))
            # add group/record info
            row = [ self.TYPE, self._grp_idx, i ] + row
            row.append(False)

            self._test_data.append(row)

        if self._populate_universe:
            row = self._test_data[0]
            row[len(row) - 1] = True
            self._pre_test_data.append(row)

    def _check_scenario(self):
        # make sure scenario & provided flags are valid
        if self._scenario not in self.SCENARIOS:
            raise Exception('Invalid scenario')
        if self._scenario == self.NO_SYS_ID and not self._populate_universe:
            raise Exception('Need to populate universe when there is no system ID')

    def _fill(self, cols):
        # Make sure following invariants are perserved.
        # 1. if NOT populating universe before test, we need > 1/2 system ID populated
        # 2. if domain/name are provided, make sure we add DUNS column to prevent fuzzy match
        #    from affecting the test
        if not self._populate_universe:
            # if we are not populating universe pre test, need to make sure we have
            # more than half (> 1/2) of system ID populated
            self._fill_half_sysid(cols)
        if DOMAIN in cols or NAME in cols:
            cols.add(DUNS)
        return cols

    def _fill_half_sysid(self, col):
        while len(col.intersection(self.SYS_ID_ONLY)) < more_than_half(len(self.SYS_ID_ONLY)):
            col.add(random.choice(list(self.SYS_ID_ONLY)))
