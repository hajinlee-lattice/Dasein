package com.latticeengines.objectapi.service.impl;


import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.objectapi.service.EventQueryService;
import com.latticeengines.objectapi.service.RatingQueryService;

/**
 * Before we have a test data that works for both date and non-date attrs,
 * we have to use a separate tests for comprehensive date attr query test
 */
public class DateAttrsQueryTestNG extends QueryServiceImplTestNGBase {

    @Inject
    private EntityQueryService entityQueryService;

    @Inject
    private EventQueryService eventQueryService;

    @Inject
    private RatingQueryService ratingQueryService;

    @BeforeClass(groups = { "functional", "manual" })
    public void setup() {
        super.setupTestData(4);
    }

    @Test(groups = "manual")
    public void testMax() {
        Set<AttributeLookup> set = new HashSet<>();
        AttributeLookup accout_1 = new AttributeLookup(BusinessEntity.Account, "user_createddate");
        AttributeLookup accout_2 = new AttributeLookup(BusinessEntity.Account, "user_testdate_dd_mmm_yyyy__8h");
        AttributeLookup accout_3 = new AttributeLookup(BusinessEntity.Account,
                "user_testdate_column_dd_mmm_yyyy_withoutdate");
        AttributeLookup contact_1 = new AttributeLookup(BusinessEntity.Contact,
                "user_testdate_column_dd_mmm_yyyy_withoutdate");
        AttributeLookup contact_2 = new AttributeLookup(BusinessEntity.Contact,
                "user_created_date_mm_dd_yyyy_hh_mm_ss_12h");
        // set.addAll(Arrays.asList(accout_2, accout_3, contact_1, contact_2));
        set.addAll(Arrays.asList(accout_2, accout_3));
        // set.addAll(Arrays.asList(contact_1, contact_2));
        Map<AttributeLookup, Object> results = ReflectionTestUtils.invokeMethod(entityQueryService, //
                "getMaxDates", set, attrRepo);
        results.forEach((k, v) -> System.out.println(k + ": " + v));
    }


    @Test(groups = "manual")
    public void testMaxViaFrontEndQuery() {
        Set<AttributeLookup> set = new HashSet<>();
        AttributeLookup accout_1 = new AttributeLookup(BusinessEntity.Account, "user_createddate");
        AttributeLookup accout_2 = new AttributeLookup(BusinessEntity.Account, "user_testdate_dd_mmm_yyyy__8h");
        AttributeLookup accout_3 = new AttributeLookup(BusinessEntity.Account,
                "user_testdate_column_dd_mmm_yyyy_withoutdate");
        AttributeLookup contact_1 = new AttributeLookup(BusinessEntity.Contact,
                "user_testdate_column_dd_mmm_yyyy_withoutdate");
        AttributeLookup contact_2 = new AttributeLookup(BusinessEntity.Contact,
                "user_created_date_mm_dd_yyyy_hh_mm_ss_12h");
        // set.addAll(Arrays.asList(accout_2, accout_3, contact_1, contact_2));
        set.addAll(Arrays.asList(accout_2, accout_3));
        // set.addAll(Arrays.asList(contact_1, contact_2));
        Map<AttributeLookup, Object> results = ReflectionTestUtils.invokeMethod(entityQueryService, //
                "getMaxDatesViaFrontEndQuery", set, DataCollection.Version.Blue);
        results.forEach((k, v) -> System.out.println(k + ": " + v));
    }


}
