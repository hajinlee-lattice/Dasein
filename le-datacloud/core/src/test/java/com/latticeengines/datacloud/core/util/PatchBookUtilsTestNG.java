package com.latticeengines.datacloud.core.util;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.DateUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils;
import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook.Type;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchBookValidationError;

public class PatchBookUtilsTestNG {
    private static final String TEST_DATE_FORMAT = "yyyy-MM-dd";
    private static final String TEST_CONSTANT_DATE = "2018-10-16";
    private static final AtomicLong TEST_COUNTER = new AtomicLong();

    @Test(groups = "unit")
    public void testDuplicateMatchKey() {
        Object[][] inputData = provideBatchData();
        List<PatchBook> patchBookList = new ArrayList<>();
        for (Object[] inputDatum : inputData) {
            PatchBook patchBook = new PatchBook();
            patchBook.setType((Type) inputDatum[0]);
            patchBook.setPid((Long) inputDatum[1]);
            patchBook.setDomain((String) inputDatum[2]);
            patchBook.setDuns((String) (inputDatum[3]));
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
                .filter(test -> !test.expectedValue)
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

    @Test(groups = "unit", dataProvider = "dateRangeEOLFlagTest")
    public void testEolFlag(DateTest test) {
        Assert.assertEquals(PatchBookUtils.isEndOfLife(test.book, newDate(TEST_CONSTANT_DATE)), test.expectedValue);
    }

    @Test(groups = "unit", dataProvider = "testStandardizeData")
    public void testInputStandardizer(PatchBook patchBook, PatchBook expectedPatchBook) {
        CountryCodeService service = Mockito.mock(CountryCodeService.class);
        Mockito.when(service.getStandardCountry(patchBook.getCountry())).thenAnswer(invocation -> {
            String countryArg = invocation.getArgument(0);
            switch (countryArg) {
                case "USA":
                    return "US";
                case "CANADA":
                    return "CAN";
                case "INDIA":
                    return "IND";
                default:
                    return "US";
            }
        });
        PatchBookUtils.standardize(patchBook, service);
        Assert.assertEquals(patchBook.getDomain(), expectedPatchBook.getDomain());
        Assert.assertEquals(patchBook.getDuns(), expectedPatchBook.getDuns());
        Assert.assertEquals(patchBook.getName(), expectedPatchBook.getName());
        Assert.assertEquals(patchBook.getCountry(), expectedPatchBook.getCountry());
        Assert.assertEquals(patchBook.getState(), expectedPatchBook.getState());
        Assert.assertEquals(patchBook.getCity(), expectedPatchBook.getCity());
        Assert.assertEquals(patchBook.getZipcode(), expectedPatchBook.getZipcode());
    }

    @Test(groups = "unit", dataProvider = "testMatchKeySupport")
    public void testValidMatchKey(Object[][] patchList) {
        List<PatchBook> patchBookList = new ArrayList<>();
        for (int i = 0; i < patchList.length; i++) {
            PatchBook patchBook = new PatchBook();
            patchBook.setType((Type) patchList[i][0]);
            patchBook.setPid((Long) patchList[i][1]);
            patchBook.setDomain((String) patchList[i][2]);
            patchBook.setDuns((String) patchList[i][3]);
            patchBook.setName((String) patchList[i][4]);
            patchBook.setCountry((String) patchList[i][5]);
            patchBook.setState((String) patchList[i][6]);
            patchBook.setCity((String) patchList[i][7]);
            patchBook.setZipcode((String) patchList[i][8]);
            patchBookList.add(patchBook);
        }
        List<PatchBookValidationError> errorList = PatchBookUtils.validateMatchKeySupport(patchBookList);
        Map<List<Long>, String> expectedMap = expectedMatchKeySupportData();
        for (PatchBookValidationError e : errorList) {
            Collections.sort(Arrays.asList(e.getMessage().split(":")[1].split(",")));
            Collections.sort(e.getPatchBookIds());
            String message = expectedMap.get(e.getPatchBookIds());
            Assert.assertEquals(e.getMessage(), message);
        }
    }

    private Map<List<Long>, String> expectedMatchKeySupportData() {
        Map<List<Long>, String> expectedMap = new HashMap<List<Long>, String>();
        expectedMap.put(Arrays.asList(19L), PatchBookUtils.UNSUPPORTED_MATCH_KEY_ERROR + "Name");
        expectedMap.put(Arrays.asList(15L), PatchBookUtils.UNSUPPORTED_MATCH_KEY_ERROR + "DUNS,Domain,Country");
        expectedMap.put(Arrays.asList(20L, 22L), PatchBookUtils.UNSUPPORTED_MATCH_KEY_ERROR + "Domain,Zipcode,Country");
        expectedMap.put(Arrays.asList(16L), PatchBookUtils.UNSUPPORTED_MATCH_KEY_ERROR + "DUNS,Domain,City");
        expectedMap.put(Arrays.asList(23L), PatchBookUtils.UNSUPPORTED_MATCH_KEY_ERROR + "DUNS,Domain");
        expectedMap.put(Arrays.asList(18L), PatchBookUtils.UNSUPPORTED_MATCH_KEY_ERROR + "DUNS,Domain,Zipcode");
        expectedMap.put(Arrays.asList(17L), PatchBookUtils.UNSUPPORTED_MATCH_KEY_ERROR + "Domain");
        expectedMap.put(Arrays.asList(21L), PatchBookUtils.UNSUPPORTED_MATCH_KEY_ERROR + "DUNS,Domain");
        return expectedMap;
    }

    private Map<String, List<Long>> expectedDataSet() {
        return ImmutableMap.of(PatchBookUtils.DUPLICATE_MATCH_KEY_ERROR + "DUNS=124124124,Domain=abc.com",
                Arrays.asList(1L, 2L, 5L), PatchBookUtils.DUPLICATE_MATCH_KEY_ERROR + "DUNS=328522482,Domain=def.com",
                Arrays.asList(3L, 6L), PatchBookUtils.DUPLICATE_MATCH_KEY_ERROR + "Domain=lmn.com",
                Arrays.asList(9L, 10L), PatchBookUtils.DUPLICATE_MATCH_KEY_ERROR + "DUNS=429489284",
                Arrays.asList(11L, 12L));
    }

    @Test(groups = "unit", dataProvider = "validatePatchedItems")
    public void testValidatePatchItems(List<PatchBook> patchBookItems,
            PatchBookValidationError[] expectedPatchBookErrors) {
        List<PatchBookValidationError> validationPatchErrors = PatchBookUtils
                .validatePatchedItems(patchBookItems);
        Assert.assertNotNull(validationPatchErrors);
        Assert.assertEquals(validationPatchErrors.size(), expectedPatchBookErrors.length);
        List<PatchBookValidationError> expectedErrorList = Arrays.stream(expectedPatchBookErrors).collect(Collectors.toList());
        Assert.assertEquals(validationPatchErrors.get(0).getMessage(),expectedErrorList.get(0).getMessage());
        // sorting to get in expected order
        Collections.sort(validationPatchErrors.get(0).getPatchBookIds());
        Collections.sort(expectedErrorList.get(0).getPatchBookIds());
        Assert.assertEquals((validationPatchErrors.get(0).getPatchBookIds()),
                expectedErrorList.get(0).getPatchBookIds());
    }

    @DataProvider(name = "validatePatchedItems")
    private Object[][] validatePatchedItems() {
        Map<String,Object> testPatchEmptyItems = new HashMap<>(); // for empty domain, duns
        testPatchEmptyItems.put(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.Domain), null);
        testPatchEmptyItems.put(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.DUNS), "132425135");
        return new Object[][] { { Arrays.asList(
                TestPatchBookUtils.newPatchBook(1L,
                        new MatchKeyTuple //
                                .Builder() //
                                        .withDomain("google.com") //
                                        .build(),
                        TestPatchBookUtils.newDomainDunsPatchItems(null, null)),
                TestPatchBookUtils.newPatchBook(2L,
                        new MatchKeyTuple //
                                .Builder() //
                                        .withDomain("yahoo.com") //
                                        .build(), testPatchEmptyItems),
                TestPatchBookUtils.newPatchBook(3L,
                        new MatchKeyTuple //
                                .Builder() //
                                        .withDomain("abc.com") //
                                        .withDuns("112211213") //
                                        .build(),
                        TestPatchBookUtils.newDomainDunsPatchItems("yahoo.com", "123456789"))),
                new PatchBookValidationError[] {
                        newError(PatchBookUtils.INVALID_PATCH_ITEMS, 1L, 2L) } },
        };
    }

    @DataProvider(name = "testMatchKeySupport")
    private Object[][] matchkeySupportData() {
        return new Object[][] {
                // Type, PatchID, Domain, DUNS, CompanyName, Country, State, City, ZipCode
            new Object[][] { // Testing Attribute Patch Validate API Match Key
                // PatchBookType = 'Attribute': Valid Entries
                {PatchBook.Type.Attribute, 1L, "abc.com", "123214242", null, null, null, null, null}, // Domain, Duns
                {PatchBook.Type.Attribute, 2L, "def.com", null, null, null, null, null, null}, // Domain
                {PatchBook.Type.Attribute, 3L, null, "452352356", null, null, null, null, null}, // Duns

                // PatchBookType = 'Attribute': Invalid Entries
                {PatchBook.Type.Attribute, 15L, "abc.com", "123214242", null, "INDIA", null, null, null}, // Domain, Duns, Country
                {PatchBook.Type.Attribute, 20L, "zillow.com", null, null, "INDIA", null, null, "1432541"}, // Domain, Country, ZipCode
                {PatchBook.Type.Attribute, 22L, "redfin.com", null, null, "USA", null, null, "21312"}, // Domain, Country, ZipCode
            },
            new Object[][] { // Testing Lookup Patch Validate API MatchKey
                // PatchBookType = 'Lookup' : DunsGuideBook Lookup : Valid Entries
                {PatchBook.Type.Lookup, 4L, null, null, "ABC Inc.", null, null, null, null}, // Name
                {PatchBook.Type.Lookup, 5L, null, null, "DEF Inc.", "USA", null, null, null}, // Name, Country
                {PatchBook.Type.Lookup, 6L, null, null, "GHI Inc.", "IND", "Gujarat", null, null}, // Name, Country, State
                {PatchBook.Type.Lookup, 7L, null, null, "JKL Inc.", "USA", null, "Sunnyvale", null}, // Name, Country, City
                {PatchBook.Type.Lookup, 8L, null, null, "JKL Inc.", "USA", "California", "Burlingame", null}, // Name, Country, State, City

                // PatchBookType = 'Lookup' : Valid Entries :
                {PatchBook.Type.Lookup, 9L, "ghi.com", null, null, null, null, null, null}, // Domain
                {PatchBook.Type.Lookup, 10L, null, "231412114", null, null, null, null, null}, // Duns
                {PatchBook.Type.Lookup, 11L, "cde.com", null, null, "CAN", null, null, null}, // Domain, Country
                {PatchBook.Type.Lookup, 12L, "fgh.com", null, null, "USA", "OREGON", null, null}, // Domain, Country, State
                {PatchBook.Type.Lookup, 13L, "lmn.com", null, null, "USA", null, null, "94404"}, // Domain, Country, ZipCode

                // PatchBookType = 'Lookup': Invalid Entries
                {PatchBook.Type.Lookup, 16L, "abc.com", "123214242", null, null, null, "Sunnyvale", null}, // Domain, Duns, City
                {PatchBook.Type.Lookup, 21L, "pqr.com", "321432543", null, null, null, null, null }, // Domain, Duns
                {PatchBook.Type.Lookup, 18L, "abc.com", "123214242", null, null, null, null, "12345"} // Domain, Duns, ZipCode
            },
            new Object[][] { // Testing Domain Patch Validate API MatchKey
                // PatchBookType = 'Domain' : Valid Entries
                {PatchBook.Type.Domain, 14L, null, "321432543", null, null, null, null, null }, // Duns

                // PatchBookType = 'Domain' : Invalid Entries
                {PatchBook.Type.Domain, 17L, "google.com", null, null, null, null, null, null }, // Domain
                {PatchBook.Type.Domain, 23L, "google.com", "123214522", null, null, null, null, null }, // Domain, Duns
                {PatchBook.Type.Domain, 19L, null, null, "ABC INC.", null, null, null, null } // CompanyName
            }
        };
    }

    private Object[][] provideBatchData() {
        return new Object[][] {
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
                { PatchBook.Type.Attribute, 12L, null, "429489284" }
            };
    }

    @DataProvider(name = "testStandardizeData")
    private Object[][] provideDataForStandardization() {
        Map<String, Object> map1 = TestPatchBookUtils.newDomainDunsPatchItems("www.yahoo.com", null); // patched item domain standardize
        map1.putAll(TestPatchBookUtils.newDomainDunsPatchItems(null, "12321314")); // patched item duns standardize

        Map<String, Object> standardizedMap1 = TestPatchBookUtils.newDomainDunsPatchItems("yahoo.com", null);
        standardizedMap1.putAll(TestPatchBookUtils.newDomainDunsPatchItems(null, "012321314"));

        Map<String, Object> map2 = TestPatchBookUtils.newDomainDunsPatchItems("def@rakuten.com", null); // patched item domain standardize
        map2.putAll(TestPatchBookUtils.newDomainDunsPatchItems(null, "1111abc11")); // patched item duns standardize -> invalid case

        Map<String, Object> standardizedMap2 = TestPatchBookUtils.newDomainDunsPatchItems("rakuten.com", null);
        standardizedMap2.putAll(TestPatchBookUtils.newDomainDunsPatchItems(null, null));

        Map<String, Object> map3 = TestPatchBookUtils.newDomainDunsPatchItems("googlecom", null); // patched item domain standardize -> invalid case
        map3.putAll(TestPatchBookUtils.newDomainDunsPatchItems(null, "123456677"));

        Map<String, Object> standardizedMap3 = TestPatchBookUtils.newDomainDunsPatchItems("null", null);
        standardizedMap3.putAll(TestPatchBookUtils.newDomainDunsPatchItems(null, "123456677"));

        return new Object[][] {
                { TestPatchBookUtils //
                        .newPatchBook(1L, new MatchKeyTuple //
                        .Builder() //
                        .withDomain("www.google.com") // domain standardize
                        .withDuns("123234") // duns standardize
                        .withName("Google Inc.") //
                        .withCountry("USA") // country standardize
                        .withState("CA") // state standardize
                        .withCity("Sunnyvale") // city standardize depending on country
                        .withZipcode("94086") //
                        .build(), map1), //
                  TestPatchBookUtils //
                        .newPatchBook(2L, new MatchKeyTuple //
                        .Builder() //
                        .withDomain("google.com") //
                        .withDuns("000123234") //
                        .withName("GOOGLE INC.") //
                        .withCountry("US") //
                        .withState("CA") //
                        .withCity("SUNNYVALE") //
                        .withZipcode("94086") //
                        .build(), standardizedMap1) }, // Duns
                { TestPatchBookUtils //
                        .newPatchBook(3L, new MatchKeyTuple //
                        .Builder() //
                        .withDomain("abc@yahoo.com") // domain standardize
                        .withDuns("123456789") //
                        .withName("Yahoo Inc.") //
                        .withCountry("INDIA") // country standardize
                        .withState("Maharashtra") //
                        .withCity("Mumbai") //
                        .withZipcode("400057") //
                        .build(), map2),
                 TestPatchBookUtils //
                     .newPatchBook(4L, new MatchKeyTuple //
                        .Builder() //
                        .withDomain("yahoo.com") //
                        .withDuns("123456789") //
                        .withName("YAHOO INC.") //
                        .withCountry("IND") //
                        .withState("MAHARASHTRA") //
                        .withCity("MUMBAI") //
                        .withZipcode("400057") //
                        .build(), standardizedMap2) },
                { TestPatchBookUtils //
                    .newPatchBook(5L, new MatchKeyTuple //
                        .Builder() //
                        .withDomain("rakutencom") // invalid domain
                        .withDuns("1111abcsk") // invalid duns
                        .withName("AAA$$@%@%") // company name standardize
                        .withCountry("CANADA") //
                        .withState("ONTARIO") //
                        .withCity("TORONTO") //
                        .withZipcode("94404") //
                        .build(), map3),
                 TestPatchBookUtils //
                     .newPatchBook(6L, new MatchKeyTuple //
                       .Builder() //
                       .withDomain(null) //
                       .withDuns("000001111") //
                       .withName("AAA") //
                       .withCountry("CAN") //
                       .withState("ONTARIO") //
                       .withCity("TORONTO") //
                       .withZipcode("94404") //
                       .build(), standardizedMap3) }
        };
    }

    @DataProvider(name = "dateRangeEOLFlagTest")
    private Object[][] provideDateRangeTestDataForEOLFlag() throws Exception {
        // NOTE ES = EffectiveSince, EA = ExpireAfter, expectedFlag = EOL flag
        return new Object[][] {
                // Case #1: Current Time within ES and EA
                { newDateTest("2018-10-15", "2018-10-20", false) }, //
                { newDateTest(null, null, false) }, //
                { newDateTest("2018-10-16", "2018-10-16", false) }, //
                // Case #2: Current Time not within ES and EA
                { newDateTest(null, "2017-10-17", true) }, //
                { newDateTest("2019-10-21", null, true) }, //
                { newDateTest("2017-10-10", "2017-10-22", true) }, //
                { newDateTest("2018-10-14", "2018-10-14", true) }, //
                // Case #3: ES is not provided
                { newDateTest(null, "2018-10-15", true) }, //
                { newDateTest(null, "2018-10-21", false) }, //
                // Case #4: EA is not provided
                { newDateTest("2018-10-15", null, false) }, //
                { newDateTest("2018-10-20", null, true) } //
        };
    }

    @DataProvider(name = "effectiveDateRangeTestData")
    private Object[][] provideEffectiveDateRangeTestData() throws Exception {
        // NOTE ES = EffectiveSince, EA = ExpireAfter, expectedFlag = whether date range is valid
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
    private DateTest newDateTest(String effectiveSince, String expireAfter, boolean expectedValue) throws Exception {
        PatchBook book = new PatchBook();
        book.setPid(TEST_COUNTER.incrementAndGet());
        book.setEffectiveSince(newDate(effectiveSince));
        book.setExpireAfter(newDate(expireAfter));
        return new DateTest(book, expectedValue);
    }

    private Date newDate(String dateStr) {
        if (dateStr == null) {
            return null;
        }
        try {
            return DateUtils.parseDate(dateStr, TEST_DATE_FORMAT);
        } catch (ParseException e) {
            throw new RuntimeException();
        }
    }

    /*
     * Test class for testInvalidEffectiveDateRange
     */
    private class DateTest {
        final PatchBook book;
        final boolean expectedValue;

        DateTest(PatchBook book, boolean expectedValue) {
            this.book = book;
            this.expectedValue = expectedValue;
        }
    }

    /*
     * helper to create validation error with patchBookIds sorted
     */
    private PatchBookValidationError newError(String msg, Long... ids) {
        PatchBookValidationError error = new PatchBookValidationError();
        error.setMessage(msg);
        // to a sorted list
        error.setPatchBookIds(Arrays.stream(ids).sorted().collect(Collectors.toList()));
        return error;
    }
}
