package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.FACEBOOK_DCS_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.FACEBOOK_DCZ_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.FACEBOOK_DC_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.FACEBOOK_DC_2;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.FACEBOOK_D_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.FACEBOOK_NCS_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.FACEBOOK_NC_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.FACEBOOK_N_1_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.FACEBOOK_N_1_2;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.FACEBOOK_N_1_3;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_DC_1_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_DC_1_2;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_DN_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_DN_2;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_DU_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_D_1_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_D_1_2;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NCC_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NCSCI_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NCSCI_2;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NCSZ_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NCS_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NCS_2;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NCZ_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NC_1_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NC_1_2;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NSCI_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NSCI_2;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NSCI_3;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NS_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_NS_2;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_N_1_1;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_N_1_2;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.GOOGLE_N_1_3;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.newDomainDunsPatchItems;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.newDunsPatchItems;
import static com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils.newPatchBook;
import static com.latticeengines.datacloud.core.util.PatchBookUtils.DUPLICATE_MATCH_KEY_ERROR;
import static com.latticeengines.datacloud.core.util.PatchBookUtils.UNSUPPORTED_MATCH_KEY_ERROR;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.exposed.util.TestPatchBookUtils;
import com.latticeengines.datacloud.match.exposed.service.PatchBookValidator;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchBookValidationError;

public class PatchBookValidatorImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    // partial error messages used for matching validation errors
    private static final String INVALID_DUNS_ERROR_MSG = "Invalid DUNS";
    private static final String INVALID_DOMAIN_ERROR_MSG = "Invalid Domain";
    private static final String SHOULD_ONLY_HAVE_DUNS_ERROR_MSG = "Should only have DUNS";
    private static final String SHOULD_ONLY_HAVE_DOMAIN_DUNS_ERROR_MSG = "Should only have Domain + DUNS";

    // patchItems for testing
    private static final Map<String, Object> VALID_DUNS_PATCH_ITEMS = newDunsPatchItems("123456789");
    private static final Map<String, Object> VALID_DOMAIN_DUNS_PATCH_ITEMS = newDomainDunsPatchItems(
            "google.com", "123456789");
    private static final Map<String, Object> INVALID_DUNS_PATCH_ITEMS = newDunsPatchItems("aabbabc");
    private static final Map<String, Object> INVALID_DOMAIN_DUNS_PATCH_ITEMS = newDomainDunsPatchItems(
            "+_-~~~!", "aabbabc");
    private static final Map<String, Object> UNKNOWN_KEY_PATCH_ITEMS = new HashMap<>();
    private static final Map<String, Object> EXTRA_KEY_DUNS_PATCH_ITEMS = new HashMap<>(VALID_DUNS_PATCH_ITEMS);
    private static final Map<String, Object> EXTRA_KEY_DOMAIN_DUNS_PATCH_ITEMS =
            new HashMap<>(VALID_DOMAIN_DUNS_PATCH_ITEMS);

    static {
        // two unknown keys
        UNKNOWN_KEY_PATCH_ITEMS.put("UnknownKey1", "UnknownValue1");
        UNKNOWN_KEY_PATCH_ITEMS.put("UnknownKey2", "UnknownValue2");
        // one unknown key on top of existing valid DUNS/Domain
        EXTRA_KEY_DUNS_PATCH_ITEMS.put("UnknownKey1", "UnknownValue1");
        EXTRA_KEY_DOMAIN_DUNS_PATCH_ITEMS.put("UnknownKey1", "UnknownValue1");
    }

    @Inject
    private PatchBookValidatorImpl validator;

    @Test(groups = "functional", dataProvider = "patchBookValidation")
    private void testPatchBookValidation(
            PatchBook.Type type, PatchBook[] books, PatchBookValidationError[] expectedErrors) {
        Pair<Integer, List<PatchBookValidationError>> validationResult = validator
                .validate(type, currentDataCloudVersion, Arrays.asList(books));
        Assert.assertNotNull(validationResult);
        List<PatchBookValidationError> errors = validationResult.getValue();
        Assert.assertNotNull(errors);
        // Assert.assertEquals(errors.size(), expectedErrors.length);

        List<PatchBookValidationError> expectedErrorList = Arrays.stream(expectedErrors).collect(Collectors.toList());
        errors.forEach(error -> verifyValidationError(error, expectedErrorList));
        // all expected errors are matched
        Assert.assertTrue(expectedErrorList.isEmpty());
    }

    /*
     * each row: [ PatchBook.Type, PatchBook[], PatchBookValidationError[] ]
     */
    @DataProvider(name = "patchBookValidation")
    private Object[][] providePatchBookValidationTestData() {
        // TODO add test data for other types and merge here
        return addType(provideLookupPatchBookValidationTestData(), PatchBook.Type.Lookup);
    }

    @DataProvider(name = "attrPatchValidateAndStandardize")
    private Object[][] dataForPatchItemValidateAndStandardize() {
        Map<String, Object> map1 = new HashMap<>();
        map1.put("Domain", "abc.com"); // column not present in AccountMasterColumn
        map1.put(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.DUNS), "123456778"); // excludedPatchItem
        map1.put(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.City), "  Sunnyvale  "); // Valid : standardize
        Map<String, Object> map2 = new HashMap<>();
        map2.put("Domain", "def.com"); // column not present in AccountMasterColumn
        map2.put(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.DUNS), "123456778  "); // excludedPatchItem
        map2.put("City", "  Mountain View  "); // column not present in AccountMasterColumn
        Map<String, Object> map4 = new HashMap<>();
        map4.put("BmbrSurge_2in1PCs_BuckScore", "test_value");
        return new Object[][] {
                {
                new PatchBook[] {
                        TestPatchBookUtils //
                            .newPatchBook(1L,new MatchKeyTuple //
                                .Builder() //
                                .withDomain("google.com") //
                                .withDuns("123234115") //
                                .withName("Google Inc.") //
                                .build(), map1),
                        TestPatchBookUtils //
                            .newPatchBook(2L,new MatchKeyTuple //
                                .Builder() //
                                .withDomain("yahoo.com") //
                                .withDuns("434343433") //
                                .withName("Yahoo Inc.") //
                                .build(), map2),
                        TestPatchBookUtils //
                        .newPatchBook(7L,
                                new MatchKeyTuple //
                                    .Builder() //
                                    .withDomain("lmn.com") //
                                    .withCountry("USA") //
                                    .build(), map4)
            },
            new PatchBookValidationError[] {
                    // Column = Domain
                            newError(PatchBookValidator.PATCH_ITEM_NOT_IN_AM + "[Domain]", 1L),
                    // Column = LDC_DUNS
                            newError(PatchBookValidator.EXCLUDED_PATCH_ITEM
                                    + "[" + MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.DUNS) + "]", 1L, 2L),
                    // Column Domain, Duns
                            newError(PatchBookValidator.PATCH_ITEM_NOT_IN_AM + "[City, Domain]",
                                    2L),
                            // Encoded Attributes not allowed to patch
                            newError(PatchBookValidator.ENCODED_ATTRS_NOT_SUPPORTED
                                    + "[BmbrSurge_2in1PCs_BuckScore]", 7L) }
                }
        };
    }

    @Test(groups = "functional", dataProvider = "attrPatchValidateAndStandardize")
    private void testAttrPatchItemsValidateAndStandardize(PatchBook[] books, PatchBookValidationError[] expectedErrors) {
        List<PatchBookValidationError> errors = validator
                .validatePatchKeyItemAndStandardize(Arrays.asList(books), currentDataCloudVersion);
        Assert.assertNotNull(errors);
        Assert.assertEquals(errors.size(), expectedErrors.length);

        List<PatchBookValidationError> expectedErrorList = Arrays.stream(expectedErrors).collect(Collectors.toList());
        errors.forEach(error -> verifyValidationError(error, expectedErrorList));
        // all expected errors are matched
        Assert.assertTrue(expectedErrorList.isEmpty());
    }

    @Test(groups = "functional", dataProvider = "patchBookMatchKeyConflict")
    private void testPatchBookMatchKeyConflict(PatchBook[] books, PatchBookValidationError[] expectedErrors) {
        List<PatchBookValidationError> errors = validator //
                .validateConflictInPatchItems(Arrays.asList(books), currentDataCloudVersion);
        Assert.assertNotNull(errors);
        Assert.assertEquals(errors.size(), expectedErrors.length);

        List<PatchBookValidationError> expectedErrorList = Arrays.stream(expectedErrors).collect(Collectors.toList());
        errors.forEach(error -> verifyValidationError(error, expectedErrorList));
        // all expected errors are matched
        Assert.assertTrue(expectedErrorList.isEmpty());
    }

    @DataProvider(name = "patchBookMatchKeyConflict")
    private Object[][] providePatchBookMatchKeyConflictData() {
        Map<String, Object> map1 = new HashMap<>();
        map1.put("AlexaRank", "100");
        Map<String, Object> map2 = new HashMap<>();
        map2.put("AlexaRank", "10");
        Map<String, Object> map3 = new HashMap<>();
        map3.put("AlexaRank", "100");
        map3.put("TotalEmployees", 100);
        return new Object[][] {
            {
                new PatchBook[] {
                        // no conflict
                        TestPatchBookUtils //
                            .newPatchBook(1L, new MatchKeyTuple //
                                    .Builder() //
                                    .withDomain("abc.com") //
                                    .withDuns("123456789") //
                                    .build(), map1),
                        // conflict with domain
                        TestPatchBookUtils //
                            .newPatchBook(2L, new MatchKeyTuple //
                                    .Builder() //
                                    .withDomain("def.com") //
                                    .withDuns("323232322") //
                                    .build(), map1),
                        TestPatchBookUtils //
                            .newPatchBook(3L, new MatchKeyTuple //
                                    .Builder() //
                                    .withDomain("def.com") //
                                    .build(), map2),
                        // conflict with duns
                        TestPatchBookUtils //
                            .newPatchBook(4L, new MatchKeyTuple //
                                    .Builder() //
                                    .withDomain("ghi.com") //
                                    .withDuns("111111111") //
                                    .build(), map1),
                        TestPatchBookUtils //
                            .newPatchBook(5L, new MatchKeyTuple //
                                    .Builder() //
                                    .withDuns("111111111") //
                                    .build(), map2),
                        TestPatchBookUtils //
                            .newPatchBook(6L, new MatchKeyTuple //
                                    .Builder() //
                                    .withDomain("abc.com") //
                                    .withCountry("USA") //
                                    .build(), map3)
                },
                new PatchBookValidationError[] {
                        // domain & duns conflict
                                newError(PatchBookValidator.CONFLICT_IN_PATCH_ITEM, 2L, 3L, 4l,
                                        5L) }
                }
        };
    }

    @Test(groups = "functional", dataProvider = "patchBookMatchKeyDomainDunsSrc")
    private void testPatchBookMatchKeyDomainDunsSrc(PatchBook[] books, String dataCloudVersion,
            PatchBookValidationError[] expectedErrors) {
        List<PatchBookValidationError> errors = validator //
                .validateSourceAttribute(Arrays.asList(books), dataCloudVersion);
        Assert.assertNotNull(errors);
        Assert.assertEquals(errors.size(), expectedErrors.length);

        List<PatchBookValidationError> expectedErrorList = Arrays.stream(expectedErrors).collect(Collectors.toList());
        errors.forEach(error -> verifyValidationError(error, expectedErrorList));
        // all expected errors are matched
        Assert.assertTrue(expectedErrorList.isEmpty());
    }

    @Test(groups = "functional", dataProvider = "domainPatchValidation")
    private void testDomainPatchValidation(PatchBook[] books, PatchBookValidationError[] expectedErrors) {
        List<PatchBookValidationError> errors = validator //
                .domainPatchValidate(Arrays.asList(books));
        Assert.assertNotNull(errors);
        Assert.assertEquals(errors.size(), expectedErrors.length);

        List<PatchBookValidationError> expectedErrorList = Arrays.stream(expectedErrors)
                .collect(Collectors.toList());
        errors.forEach(error -> verifyValidationError(error, expectedErrorList));
        // all expected errors are matched
        Assert.assertTrue(expectedErrorList.isEmpty());
    }

    @DataProvider(name = "domainPatchValidation")
    private Object[][] domainPatchValidation() {
        Map<String, Object> map1 = new HashMap<>();
        map1.put(DataCloudConstants.ATTR_LDC_DOMAIN, "abc.com");
        Map<String, Object> map2 = new HashMap<>();
        map2.put(DataCloudConstants.ATTR_LDC_DOMAIN, "def.com");
        map2.put(DataCloudConstants.ATTR_LDC_INDUSTRY, "DEF");
        Map<String, Object> map3 = new HashMap<>();
        map3.put(DataCloudConstants.ATTR_LDC_DOMAIN, "google.com");
        Map<String, Object> map4 = new HashMap<>();
        map4.put(DataCloudConstants.ATTR_LDC_NAME, "AAA");
        return new Object[][] { {
                new PatchBook[] {
                        // correct entry
                        TestPatchBookUtils //
                                .newPatchBook(2L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDuns("514513113") //
                                                        .build(),
                                        map1),
                        TestPatchBookUtils //
                                .newPatchBook(9L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDuns("333333333") //
                                                        .build(),
                                        map1),
                        // error entry : patch items besides domain
                        TestPatchBookUtils //
                                .newPatchBook(7L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDuns("232423133") //
                                                        .build(),
                                        map2),
                        TestPatchBookUtils //
                                .newPatchBook(8L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDuns("323133313") //
                                                        .build(),
                                        map4),
                        // error entry : duplicate duns
                        TestPatchBookUtils //
                                .newPatchBook(1L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDuns("123456789") //
                                                        .build(),
                                        map1),
                        TestPatchBookUtils //
                                .newPatchBook(3L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDuns("123456789") //
                                                        .build(),
                                        map1),
                        TestPatchBookUtils //
                                .newPatchBook(4L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDuns("333333333") //
                                                        .build(),
                                        map3),
                        TestPatchBookUtils //
                                .newPatchBook(5L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDuns("333333333") //
                                                        .build(),
                                        map3),
                        TestPatchBookUtils //
                                .newPatchBook(6L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDuns("333333333") //
                                                        .build(),
                                        map3),
                },
                new PatchBookValidationError[] {
                        newError(PatchBookValidator.DUPLI_MATCH_KEY_AND_PATCH_ITEM_COMBO
                                + "DUNS = 123456789 PatchDomain = abc.com", 1L, 3L),
                        newError(
                                PatchBookValidator.DUPLI_MATCH_KEY_AND_PATCH_ITEM_COMBO
                                        + "DUNS = 333333333 PatchDomain = google.com",
                                4L, 5L, 6L),
                        newError(
                                PatchBookValidator.ERR_IN_PATCH_ITEMS, 7L, 8L) }, } };
    }

    @DataProvider(name = "patchBookMatchKeyDomainDunsSrc")
    private Object[][] patchBookMatchKeyDomainDunsSrc() {
        Map<String, Object> map1 = new HashMap<>();
        map1.put("AlexaAUPageViews", "100");
        map1.put("AlexaUSRank", "110");
        Map<String, Object> map2 = new HashMap<>();
        map2.put("AlexaUSRank", "10");
        Map<String, Object> map3 = new HashMap<>();
        map3.put("BEMFAB", "1");
        map3.put("DnB_CITY_CODE", "1111");
        Map<String, Object> map4 = new HashMap<>();
        map4.put("BEMFAB", "2");
        return new Object[][] { {
                new PatchBook[] {
                        // Match key maps appropriately to domain / duns based
                        // source attributes
                        TestPatchBookUtils //
                                .newPatchBook(1L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDomain("abc.com") //
                                                        .withDuns("123456789") //
                                                        .build(),
                                        map1),
                        TestPatchBookUtils //
                                .newPatchBook(2L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDomain("def.com") //
                                                        .withDuns("323232322") //
                                                        .build(),
                                        map1),
                        TestPatchBookUtils //
                                .newPatchBook(3L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDomain("def.com") //
                                                        .withDuns("121331131") //
                                                        .build(),
                                        map2),
                        TestPatchBookUtils //
                                .newPatchBook(4L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDomain("ghi.com") //
                                                        .withDuns("111111111") //
                                                        .build(),
                                        map1),
                        // Match key doesnt match corresponding domain / duns
                        // based source attributes
                        // single attribute
                        TestPatchBookUtils //
                                .newPatchBook(5L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDuns("111111111") //
                                                        .build(),
                                        map2),
                        TestPatchBookUtils //
                                .newPatchBook(8L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDomain("yyt.com") //
                                                        .build(),
                                        map4),
                        // multiple attributes
                        TestPatchBookUtils //
                                .newPatchBook(7L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDuns("111111111") //
                                                        .build(),
                                        map1),
                        TestPatchBookUtils //
                                .newPatchBook(6L,
                                        new MatchKeyTuple //
                                                .Builder() //
                                                        .withDomain("abc.com") //
                                                        .withCountry("USA") //
                                                        .build(),
                                        map3) },
                "2.0.16",
                new PatchBookValidationError[] {
                        newError(PatchBookValidator.ATTRI_PATCH_DOM_BASED_SRC_ERR + "[AlexaUSRank]",
                                5L),
                        newError(PatchBookValidator.ATTRI_PATCH_DUNS_BASED_SRC_ERR
                                + "[BEMFAB, DnB_CITY_CODE]", 6L),
                        newError(PatchBookValidator.ATTRI_PATCH_DOM_BASED_SRC_ERR + ""
                                + "[AlexaAUPageViews, AlexaUSRank]", 7L),
                        newError(PatchBookValidator.ATTRI_PATCH_DUNS_BASED_SRC_ERR
                                + "[BEMFAB]", 8L) }, } };
    }

    private Object[][] provideLookupPatchBookValidationTestData() {
        return new Object[][] {
                // Case #1: Duplicate match keys (standardized to the same value)
                {
                        // NOTE some match keys are already duplicates before standardization, some only becomes
                        // duplicates after standardization
                        new PatchBook[] {
                                // Standardized to Name=google
                                newPatchBook(100L, GOOGLE_N_1_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(101L, GOOGLE_N_1_2, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(102L, GOOGLE_N_1_3, VALID_DUNS_PATCH_ITEMS),
                                // Standardized to Name=google,Country=USA
                                newPatchBook(110L, GOOGLE_NC_1_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(111L, GOOGLE_NC_1_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(112L, GOOGLE_NC_1_2, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(113L, GOOGLE_NC_1_2, VALID_DUNS_PATCH_ITEMS),
                                // Standardized to Name=facebook,Country=USA
                                newPatchBook(120L, FACEBOOK_N_1_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(121L, FACEBOOK_N_1_2, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(122L, FACEBOOK_N_1_3, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(123L, FACEBOOK_N_1_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(124L, FACEBOOK_N_1_2, VALID_DUNS_PATCH_ITEMS),
                                // Standardized to Domain=google.com
                                newPatchBook(130L, GOOGLE_D_1_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(131L, GOOGLE_D_1_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(132L, GOOGLE_D_1_2, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(133L, GOOGLE_D_1_2, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                // Standardized to Domain=google.com,Country=USA
                                newPatchBook(140L, GOOGLE_DC_1_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(141L, GOOGLE_DC_1_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(142L, GOOGLE_DC_1_2, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                        }, new PatchBookValidationError[] {
                                // Name=google
                                newError(DUPLICATE_MATCH_KEY_ERROR, 100L, 101L, 102L),
                                // Name=google,Country=USA
                                newError(DUPLICATE_MATCH_KEY_ERROR, 110L, 111L, 112L, 113L),
                                // Name=facebook,Country=USA
                                newError(DUPLICATE_MATCH_KEY_ERROR, 120L, 121L, 122L, 123L, 124L),
                                // Domain=google.com
                                newError(DUPLICATE_MATCH_KEY_ERROR, 130L, 131L, 132L, 133L),
                                // Domain=google.com,Country=USA
                                newError(DUPLICATE_MATCH_KEY_ERROR, 140L, 141L, 142L),
                        }
                },
                // Case #2: Unsupported Match Keys
                {
                        new PatchBook[] {
                                // Name,State
                                newPatchBook(200L, GOOGLE_NS_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(201L, GOOGLE_NS_2, VALID_DUNS_PATCH_ITEMS),
                                // Name,Country,Zipcode
                                newPatchBook(210L, GOOGLE_NCZ_1, VALID_DUNS_PATCH_ITEMS),
                                // Name,Country,State,Zipcode
                                newPatchBook(220L, GOOGLE_NCSZ_1, VALID_DUNS_PATCH_ITEMS),
                                // Name,State,City
                                newPatchBook(230L, GOOGLE_NSCI_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(231L, GOOGLE_NSCI_2, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(232L, GOOGLE_NSCI_3, VALID_DUNS_PATCH_ITEMS),
                                // Domain,Name
                                newPatchBook(240L, GOOGLE_DN_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(241L, GOOGLE_DN_2, VALID_DUNS_PATCH_ITEMS),
                        }, new PatchBookValidationError[] {
                                // Name,State
                                newError(UNSUPPORTED_MATCH_KEY_ERROR, 200L, 201L),
                                // Name,Country,Zipcode
                                newError(UNSUPPORTED_MATCH_KEY_ERROR, 210L),
                                // Name,Country,State,Zipcode
                                newError(UNSUPPORTED_MATCH_KEY_ERROR, 220L),
                                // Name,State,City
                                newError(UNSUPPORTED_MATCH_KEY_ERROR, 230L, 231L, 232L),
                                // Domain,Name
                                newError(UNSUPPORTED_MATCH_KEY_ERROR, 240L, 241L),
                        }
                },
                // Case #3: Invalid PatchItems for SUPPORTED Match Keys
                {
                        // NOTE IDUNS = Invalid DUNS, IDomain = Invalid Domain
                        new PatchBook[] {
                                // MatchKey=Name, patchItems={IDUNS}, expectedPatchItems={DUNS}
                                newPatchBook(300L, GOOGLE_N_1_1, INVALID_DUNS_PATCH_ITEMS),
                                // MatchKey=Name, patchItems={DUNS,Domain}, expectedPatchItems={DUNS}
                                newPatchBook(301L, FACEBOOK_N_1_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                // MatchKey=Name,Country, patchItems={IDUNS}, expectedPatchItems={DUNS}
                                newPatchBook(302L, GOOGLE_NC_1_1, INVALID_DUNS_PATCH_ITEMS),
                                // MatchKey=Name,Country,State, patchItems={DUNS,Domain}, expectedPatchItems={DUNS}
                                // => got an extra field (Domain)
                                newPatchBook(303L, GOOGLE_NCS_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                // MatchKey=Name,Country,State,City, patchItems={UNKNOWN}, expectedPatchItems={DUNS}
                                // => Invalid DUNS (because no DUNS field), NO should only have duns error because
                                // # of patch items is 1
                                newPatchBook(304L, GOOGLE_NCSCI_1, UNKNOWN_KEY_PATCH_ITEMS),
                                // MatchKey=Domain,Country, patchItems={IDUNS,IDomain}, expectedPatchItems={DUNS,Domain}
                                newPatchBook(305L, GOOGLE_DC_1_1, INVALID_DOMAIN_DUNS_PATCH_ITEMS),
                                // MatchKey=Name,Country, patchItems={DUNS,UNKNOWN}, expectedPatchItems={DUNS}
                                // => got should only have duns error because # of patch items != 1
                                newPatchBook(306L, FACEBOOK_NC_1, EXTRA_KEY_DUNS_PATCH_ITEMS),
                                // MatchKey=Domain, patchItems={DUNS,Domain,UNKNOWN}, expectedPatchItems={DUNS,Domain}
                                // => got should only have duns error because # of patch items != 2
                                newPatchBook(307L, FACEBOOK_D_1, EXTRA_KEY_DOMAIN_DUNS_PATCH_ITEMS),
                        }, new PatchBookValidationError[] {
                                newError(INVALID_DUNS_ERROR_MSG, 300L, 302L, 304L, 305L),
                                newError(SHOULD_ONLY_HAVE_DUNS_ERROR_MSG, 301L, 303L, 304L, 306L),
                                newError(INVALID_DOMAIN_ERROR_MSG, 305L),
                                newError(SHOULD_ONLY_HAVE_DOMAIN_DUNS_ERROR_MSG, 307L),
                        }
                },
                // Case #4: Mixing entries from #1, #2, #3 and valid entries
                {
                        new PatchBook[] {
                                // Case #1
                                newPatchBook(400L, GOOGLE_NC_1_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(401L, GOOGLE_NC_1_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(402L, GOOGLE_NC_1_2, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(403L, GOOGLE_NC_1_2, VALID_DUNS_PATCH_ITEMS),

                                // Case #2
                                newPatchBook(410L, GOOGLE_NS_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(411L, GOOGLE_NS_2, VALID_DUNS_PATCH_ITEMS),

                                // Case #3
                                newPatchBook(420L, GOOGLE_N_1_1, INVALID_DUNS_PATCH_ITEMS),
                                newPatchBook(421L, FACEBOOK_N_1_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(422L, GOOGLE_DC_1_1, INVALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(423L, FACEBOOK_NC_1, EXTRA_KEY_DUNS_PATCH_ITEMS),
                                newPatchBook(424L, FACEBOOK_D_1, EXTRA_KEY_DOMAIN_DUNS_PATCH_ITEMS),

                                // valid entries
                                // AMLookup match keys
                                newPatchBook(430L, GOOGLE_D_1_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(431L, FACEBOOK_DC_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(432L, FACEBOOK_DCS_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(433L, FACEBOOK_DCZ_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(434L, GOOGLE_DU_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                // DunsGuideBook match keys
                                newPatchBook(435L, GOOGLE_NCS_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(436L, GOOGLE_NCS_2, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(437L, GOOGLE_NCSCI_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(438L, GOOGLE_NCSCI_2, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(439L, FACEBOOK_NCS_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(440L, GOOGLE_NCC_1, VALID_DUNS_PATCH_ITEMS),
                        }, new PatchBookValidationError[] {
                                // Case #1
                                newError(DUPLICATE_MATCH_KEY_ERROR, 400L, 401L, 402L, 403L),
                                // Case #2
                                newError(UNSUPPORTED_MATCH_KEY_ERROR, 410L, 411L),
                                // Case #3
                                newError(INVALID_DUNS_ERROR_MSG, 420L, 422L),
                                newError(SHOULD_ONLY_HAVE_DUNS_ERROR_MSG, 421L, 423L),
                                newError(INVALID_DOMAIN_ERROR_MSG, 422L),
                                newError(SHOULD_ONLY_HAVE_DOMAIN_DUNS_ERROR_MSG, 424L),
                        }
                },
                // Case #5: All valid entries
                {
                        new PatchBook[] {
                                // AMLookup match keys (Domain, DUNS, Domain + Country, Domain + Country + State,
                                // Domain + Country + Zipcode)
                                newPatchBook(500L, GOOGLE_D_1_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(501L, FACEBOOK_DC_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(502L, FACEBOOK_DC_2, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(503L, FACEBOOK_DCS_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(504L, GOOGLE_DC_1_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(505L, FACEBOOK_DCZ_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                newPatchBook(506L, GOOGLE_DU_1, VALID_DOMAIN_DUNS_PATCH_ITEMS),
                                // DunsGuideBook match keys (Name, Name + Country, Name + Country + State,
                                // Name + Country + State + City, Name + Country + City)
                                newPatchBook(510L, GOOGLE_NCS_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(511L, GOOGLE_NCS_2, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(512L, GOOGLE_NCSCI_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(513L, GOOGLE_NCSCI_2, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(514L, FACEBOOK_NCS_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(515L, GOOGLE_NCC_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(516L, GOOGLE_N_1_1, VALID_DUNS_PATCH_ITEMS),
                                newPatchBook(517L, GOOGLE_NC_1_1, VALID_DUNS_PATCH_ITEMS),
                        }, new PatchBookValidationError[0]
                }
        };
    }

    /*
     * Verify if the input error matches one of the expectedError and remove the matched error from the expected list.
     * Failed the test if no matching expected error is found.
     *
     * (a) error message "contains" one of the expected validation error and
     * (b) the patchBookIds of that expected error matches current error
     */
    private void verifyValidationError(
            PatchBookValidationError error, List<PatchBookValidationError> expectedErrors) {
        Assert.assertNotNull(error);
        Assert.assertNotNull(error.getMessage());
        Assert.assertNotNull(error.getPatchBookIds());
        Assert.assertFalse(error.getPatchBookIds().isEmpty());
        Collections.sort(error.getPatchBookIds());
        Iterator<PatchBookValidationError> it = expectedErrors.iterator();
        while (it.hasNext()) {
            PatchBookValidationError expectedError = it.next();
            if (error.getMessage().contains(expectedError.getMessage())
                    && expectedError.getPatchBookIds().equals(error.getPatchBookIds())) {
                // remove the error so that each expected error can only be matched once
                it.remove();
                return;
            }
        }
        Assert.fail(String.format(
                "Unexpected ValidationError(message=%s,patchBookIds=%s)",
                error.getMessage(), error.getPatchBookIds()));
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

    /*
     * Set the given PatchBook.Type to every PatchBook entries in testData.
     *
     * testData = [ PatchBook[], PatchBookValidationError[] ]
     */
    private Object[][] addType(Object[][] testData, PatchBook.Type type) {
        Arrays.stream(testData)
                .forEach(row -> Arrays.stream((PatchBook[]) row[0])
                        .forEach(book -> book.setType(type)));
        return Arrays.stream(testData).map(row -> {
            Object[] rowWithType = new Object[row.length + 1];
            // shift by one item
            System.arraycopy(row, 0, rowWithType, 1, row.length);
            rowWithType[0] = type;
            return rowWithType;
        }).toArray(Object[][]::new);
    }
}
