package com.latticeengines.datacloud.match.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.DateUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook.Type;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchBookValidationError;

public class PatchBookUtilsTestNG {
    private static final String DUPLICATE_MATCH_KEY_ERROR = "Duplicate match key combination found : ";
    private static final String TEST_DATE_FORMAT = "yyyy-MM-dd";
    private static final AtomicLong TEST_COUNTER = new AtomicLong();

    @Test(groups = "unit")
    public void testDuplicateMatchKey() {
        Object[][] inputData = provideBatchData();
        List<PatchBook> patchBookList = new ArrayList<>();
        for (int row = 0; row < inputData.length; row++) {
            PatchBook patchBook = new PatchBook();
            patchBook.setType((Type) inputData[row][0]);
            patchBook.setPid((Long) inputData[row][1]);
            patchBook.setDomain((String) inputData[row][2]);
            patchBook.setDuns((String) (inputData[row][3]));
            patchBookList.add(patchBook);
        }
        List<PatchBookValidationError> errorList = PatchBookUtils.validateDuplicateMatchKey(patchBookList);
        // expected
        Map<String, List<Long>> expectedMap = expectedDataSet();
        for (PatchBookValidationError e : errorList) {
            List<Long> pidList = expectedMap.get(e.getMessage());
            Assert.assertNotNull(pidList);
            Collections.sort(e.getPatchBookIds());
            Assert.assertEquals(e.getPatchBookIds(), pidList);
        }
    }

    @Test(groups = "unit", dataProvider = "effectiveDateRangeTestData")
    public void testInvalidEffectiveDateRange(List<DateTest> tests) {
        List<PatchBook> books = tests.stream().map(test -> test.book).collect(Collectors.toList());
        List<Long> expectedErrorIds = tests
                .stream()
                .filter(test -> !test.isValid)
                .map(test -> test.book.getPid())
                .collect(Collectors.toList());
        List<PatchBookValidationError> errors = PatchBookUtils.validateEffectiveDateRange(books);
        Assert.assertNotNull(errors);
        if (expectedErrorIds.isEmpty()) {
            Assert.assertTrue(errors.isEmpty());
        } else {
            // currently only one type of error message
            Assert.assertEquals(errors.size(), 1);
            PatchBookValidationError err = errors.get(0);
            Assert.assertNotNull(err);
            Assert.assertNotNull(err.getMessage());
            Assert.assertNotNull(err.getPatchBookIds());

            // validate IDs
            Collections.sort(err.getPatchBookIds());
            Assert.assertEquals(err.getPatchBookIds(), expectedErrorIds);
        }
    }

    private Map<String, List<Long>> expectedDataSet() {
        return ImmutableMap.of(DUPLICATE_MATCH_KEY_ERROR + "DUNS=124124124,Domain=abc.com", Arrays.asList(1L, 2L, 5L),
                DUPLICATE_MATCH_KEY_ERROR + "DUNS=328522482,Domain=def.com", Arrays.asList(3L, 6L),
                DUPLICATE_MATCH_KEY_ERROR + "Domain=lmn.com", Arrays.asList(9L, 10L),
                DUPLICATE_MATCH_KEY_ERROR + "DUNS=429489284", Arrays.asList(11L, 12L));
    }

    private Object[][] provideBatchData() {
        return new Object[][] { // Testing Attribute Patch Validate API Match Key
                // Duplicate domain+duns
                { PatchBook.Type.Attribute, 1L, "abc.com", "124124124" },
                { PatchBook.Type.Attribute, 2L, "abc.com", "124124124" },
                { PatchBook.Type.Attribute, 5L, "abc.com", "124124124" },
                { PatchBook.Type.Attribute, 3L, "def.com", "328522482" },
                { PatchBook.Type.Attribute, 6L, "def.com", "328522482" },
                // distinct domain only
                { PatchBook.Type.Attribute, 4L, "abc.com", null },
                // distinct duns only
                { PatchBook.Type.Attribute, 7L, null, "124124124" },
                // distinct domain + duns
                { PatchBook.Type.Attribute, 8L, "ghi.com", "127947873" },
                // Duplicate domain only
                { PatchBook.Type.Attribute, 9L, "lmn.com", null },
                { PatchBook.Type.Attribute, 10L, "lmn.com", null },
                // Duplicate duns only
                { PatchBook.Type.Attribute, 11L, null, "429489284" },
                { PatchBook.Type.Attribute, 12L, null, "429489284" }, };
    }

    @DataProvider(name = "effectiveDateRangeTestData")
    private Object[][] provideEffectiveDateRangeTestData() throws Exception {
        // NOTE ES = EffectiveSince, EA = ExpireAfter
        return new Object[][] {
                // Case #1: All valid
                toObjectArray(
                        newDateTest("2018-10-18", "2018-10-18", true), // ES == EA
                        newDateTest(null, "2018-10-18", true), // null ES
                        newDateTest("2018-10-18", null, true), // null EA
                        newDateTest(null, null, true), // null ES and EA
                        newDateTest("2018-10-18", "2018-10-20", true) // ES < EA
                ),
                // Case #2: Some invalid entries
                {
                        Arrays.asList(
                                newDateTest("2018-10-18", "2018-10-18", true), // Valid
                                newDateTest(null, "2018-10-18", true), // Valid
                                newDateTest("2018-10-18", "2011-01-01", false), // Invalid
                                newDateTest(null, null, true), // Valid
                                newDateTest("2018-10-20", "2018-10-18", false) // Invalid
                        )
                },
                // Case #3: All invalid entries
                {
                        Arrays.asList(
                                newDateTest("2018-10-18", "2011-01-01", false), //
                                newDateTest("2018-10-20", "2018-10-18", false) //
                        )
                },
        };
    }

    // helper for syntax
    private Object[] toObjectArray(DateTest... tests) {
        return new Object[] { Arrays.asList(tests) };
    }

    // helper to create DateTest, effectiveSince and expireAfter can be null
    private DateTest newDateTest(String effectiveSince, String expireAfter, boolean isValid) throws Exception {
        PatchBook book = new PatchBook();
        book.setPid(TEST_COUNTER.incrementAndGet());
        book.setEffectiveSince(newDate(effectiveSince));
        book.setExpireAfter(newDate(expireAfter));
        return new DateTest(book, isValid);
    }

    private Date newDate(String dateStr) throws Exception {
        if (dateStr == null) {
            return null;
        }
        return DateUtils.parseDate(dateStr, TEST_DATE_FORMAT);
    }

    /*
     * Test class for testInvalidEffectiveDateRange
     */
    private class DateTest {
        final PatchBook book;
        final boolean isValid;
        DateTest(PatchBook book, boolean isValid) {
            this.book = book;
            this.isValid = isValid;
        }
    }
}
