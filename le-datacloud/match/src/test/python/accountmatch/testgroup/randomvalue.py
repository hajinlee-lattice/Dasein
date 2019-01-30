import random

from lib.constants import MATCH_KEY_COLS
import lib.matchkeys as matchkeys


class Group(object):
    TYPE = 'random'

    def __init__(self, grp_idx, num_records, cols_generator, value_generator,
                populate_probability=0.0, enforce_match_key_combination=False):
        """Generate records using given column/value generator. Randomly put records into pre test
        data based on given probability.

        Arguments:
            grp_idx {int} -- test group index
            num_records {int} -- number of records generated
            cols_generator -- generator function that produces a set of column names
            value_generator -- generator function that produces one record that contains all columns

        Keyword Arguments:
            populate_probability {float} -- probability that each record is put into pre test data (default: {0.0})
            enforce_match_key_combination {bool} -- whether to ensure generated columns contain valid match key combination (default: {False})
        """
        self._grp_idx = grp_idx
        self._num_records = num_records
        self._test_data = []
        self._pre_test_data = []
        self._generated = False
        self._cols_generator = cols_generator
        self._value_generator = value_generator
        self._populate_probability = populate_probability
        self._enforce = enforce_match_key_combination
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

        # make sure not generate test data twice
        self._generated = True

        for i in range(0, self._num_records):
            cols = next(self._cols_generator)
            if self._enforce:
                # make sure match key combination is valid
                matchkeys.fill_missing_col(cols)
            # defensive copy
            row = list(next(self._value_generator))
            for j, key in enumerate(MATCH_KEY_COLS):
                if key not in cols:
                    # clear out fields we don't want
                    row[j] = None

            # add group/record info
            row = [self.TYPE, self._grp_idx, i] + row
            # generate exists in universe flag
            if random.random() < self._populate_probability:
                row.append(True)
            else:
                row.append(False)
            self._test_data.append(row)

        # rows to populate in universe before test
        self._pre_test_data = list(filter(lambda x: x[len(x) - 1], self._test_data))

    def _check(self):
        # TODO implement
        pass
