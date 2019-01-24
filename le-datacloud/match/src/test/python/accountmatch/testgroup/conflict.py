import random

from lib.constants import MATCH_KEY_COLS
import lib.matchkeys as matchkeys
import lib.generators as generators


class Group(object):
    TYPE = 'controlled_conflict'

    def __init__(self, grp_idx, num_records, primary_key_value,
                 secondary_keys, value_generator, populate_universe=False):
        """Generate records that have a common value in one column, and multiple values in one or
        more other columns (to cause conflict).

        Arguments:
            grp_idx {int} -- test group index
            num_records {int} -- number of generated test records
            primary_key_value {tuple(str, str)} -- primary column name/value
                                                   (all generated records will have this column/value)
            secondary_keys {set(str)} -- set of columns that will have different values
            value_generator -- generator function that generate one record at a time
                               (which will be used to fill up secondary columns)

        Keyword Arguments:
            populate_universe {bool} -- if True, one of the columns will be put into pre test data (default: {False})
        """

        # TODO add some flag to track whether there exists secondary keys with higher priority than primary key
        self._grp_idx = grp_idx
        self._num_records = num_records
        self._test_data = []
        self._pre_test_data = []
        self._populate_universe = populate_universe
        self._primary_key, self._primary_value = primary_key_value
        self._secondary_keys = secondary_keys
        matchkeys.fill_missing_col(self._secondary_keys)
        self._value_generator = value_generator
        self._generated = False
        self._check()

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

        for i in range(0, self._num_records):
            # defensive copy
            row = list(next(self._value_generator))
            for j, key in enumerate(MATCH_KEY_COLS):
                if key == self._primary_key:
                    # set primary value
                    row[j] = self._primary_value
                elif key not in self._secondary_keys:
                    # clear out fields we don't want
                    row[j] = None

            # add group/record info
            row = [self.TYPE, self._grp_idx, i] + row
            row.append(False)
            self._test_data.append(row)

        if self._populate_universe:
            row = self._test_data[0]
            row[len(row) - 1] = True
            self._pre_test_data.append(row)

    def _check(self):
        # TODO implement
        pass
