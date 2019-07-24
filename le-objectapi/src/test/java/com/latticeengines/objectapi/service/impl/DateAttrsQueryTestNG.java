package com.latticeengines.objectapi.service.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.objectapi.service.EntityQueryService;

/**
 * Before we have a test data that works for both date and non-date attrs, we
 * have to use a separate tests for comprehensive date attr query test
 */
public class DateAttrsQueryTestNG extends QueryServiceImplTestNGBase {

    private final class AccountAttr {
        static final String TestDate1 = "user_TestDate_DD_MMM_YYYY_00_00_0024H__8H";
        static final String TestDate2 = "user_TestDate_Column_dd_mmm_yyyy_withoutDate";
    }

    private final class ContactAttr {
        static final String TestDate1 = "user_TestDate_Column_dd_mmm_yyyy_withoutDate";
        static final String TestDate2 = "user_Created_Date_mm_dd_yyyy_hh_mm_ss_12h";
    }

    @Inject
    private EntityQueryService entityQueryService;

    @BeforeClass(groups = { "manual" })
    public void setup() {
        super.setupTestData(4);
    }

    @Test(groups = "manual")
    public void testMaxViaFrontEndQuery() {
        AttributeLookup accout_2 = new AttributeLookup(BusinessEntity.Account, AccountAttr.TestDate1);
        AttributeLookup accout_3 = new AttributeLookup(BusinessEntity.Account, AccountAttr.TestDate2);
        AttributeLookup contact_1 = new AttributeLookup(BusinessEntity.Contact, ContactAttr.TestDate1);
        AttributeLookup contact_2 = new AttributeLookup(BusinessEntity.Contact, ContactAttr.TestDate2);
        Set<AttributeLookup> set = new HashSet<>(Arrays.asList(accout_2, accout_3, contact_1, contact_2));
        Map<AttributeLookup, Object> results = ReflectionTestUtils.invokeMethod(entityQueryService, //
                "getMaxDatesViaFrontEndQuery", set, attrRepo);
        Assert.assertNotNull(results);
        results.forEach((k, v) -> System.out.println(k + ": " + v));
    }

}
