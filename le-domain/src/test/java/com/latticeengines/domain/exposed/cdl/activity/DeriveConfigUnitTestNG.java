package com.latticeengines.domain.exposed.cdl.activity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DeriveConfigUnitTestNG {

    private static final String DERIVED_NAME_1 = "user 1 visited contents";
    private static final String DERIVED_NAME_1_HASH = DimensionGenerator.hashDimensionValue(DERIVED_NAME_1);
    private static final String DERIVED_NAME_2 = "user 2 visited news";
    private static final String DERIVED_NAME_2_HASH = DimensionGenerator.hashDimensionValue(DERIVED_NAME_2);
    private static final String DERIVED_NAME_3 = "user 3 visited something";
    private static final String DERIVED_NAME_3_HASH = DimensionGenerator.hashDimensionValue(DERIVED_NAME_3);

    private static final String DERIVE_SRC_1 = "UserId";
    private static final String DERIVE_SRC_2 = "WebVisitPageUrl";

    DeriveConfig deriveConfig;

    @BeforeMethod
    public void setUp() {
        deriveConfig = getDeriveConfig();
    }

    @Test(dataProvider = "valsForIdProvider")
    public void testFindDimensionId(List<String> vals, String expected) {
        Assert.assertEquals(deriveConfig.findDimensionId(vals), expected);
    }

    @Test(dataProvider = "valuesForNameProvider")
    public void testFindDimensionName(List<String> vals, String expected) {
        Assert.assertEquals(deriveConfig.findDimensionName(vals), expected);
    }

    @DataProvider
    public static Object[][] valsForIdProvider() {
        return new Object[][] {
                {Arrays.asList("test_user_u1", "example.com/contents"), DERIVED_NAME_1_HASH}, //
                {Arrays.asList("some_user_u1", "something.com/contents"), DERIVED_NAME_1_HASH}, //
                {Arrays.asList("u1", "something.com/contents"), DERIVED_NAME_1_HASH}, //
                {Arrays.asList("u1", "something.com/news"), null}, //
                {Arrays.asList("u2_test_user", "something.com/news"), DERIVED_NAME_2_HASH}, //
                {Arrays.asList("u2", "something.com/news"), DERIVED_NAME_2_HASH}, //
                {Arrays.asList("u2", "something.com/contents"), null}, //
                {Arrays.asList("u3", "www.google.com"), DERIVED_NAME_3_HASH}, //
                {Arrays.asList("u3", "www.baidu.com"), DERIVED_NAME_3_HASH}, //
                {Arrays.asList("u3", "www.example.com"), null} //
        };
    }

    @DataProvider
    public static Object[][] valuesForNameProvider() {
        return new Object[][] {
                {Arrays.asList("test_user_u1", "example.com/contents"), DERIVED_NAME_1}, //
                {Arrays.asList("some_user_u1", "something.com/contents"), DERIVED_NAME_1}, //
                {Arrays.asList("u1", "something.com/contents"), DERIVED_NAME_1}, //
                {Arrays.asList("u1", "something.com/news"), null}, //
                {Arrays.asList("u2_test_user", "something.com/news"), DERIVED_NAME_2}, //
                {Arrays.asList("u2", "something.com/news/123"), DERIVED_NAME_2}, //
                {Arrays.asList("u2", "something.com/contents"), null}, //
                {Arrays.asList("u3", "www.google.com"), DERIVED_NAME_3}, //
                {Arrays.asList("u3", "www.baidu.com"), DERIVED_NAME_3}, //
                {Arrays.asList("u3", "www.example.com"), null}, //
                {Arrays.asList("u3", ""), DERIVED_NAME_3} //
        };
    }

    private DeriveConfig getDeriveConfig() {
        DeriveConfig config = new DeriveConfig();
        config.sourceAttrs = Arrays.asList(DERIVE_SRC_1, DERIVE_SRC_2);
        config.patterns = new ArrayList<>();
        config.patterns.add(Arrays.asList(DERIVED_NAME_1, ".*u1", ".*/contents"));
        config.patterns.add(Arrays.asList(DERIVED_NAME_2, "u2.*", ".*/news.*"));
        config.patterns.add(Arrays.asList(DERIVED_NAME_2, "u2.*", ".*/news.*"));
        config.patterns.add(Arrays.asList(DERIVED_NAME_3, "u3", "(www.google.com|www.baidu.com)"));
        config.patterns.add(Arrays.asList(DERIVED_NAME_3, "u3", "\\A\\z"));
        return config;
    }
}
