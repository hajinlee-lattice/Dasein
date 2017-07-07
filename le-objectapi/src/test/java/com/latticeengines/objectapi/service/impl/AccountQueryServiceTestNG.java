package com.latticeengines.objectapi.service.impl;

import java.util.Arrays;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.objectapi.functionalframework.ObjectApiFunctionalTestNGBase;
import com.latticeengines.objectapi.service.AccountQueryService;

public class AccountQueryServiceTestNG extends ObjectApiFunctionalTestNGBase {

    @Autowired
    private AccountQueryService accountQueryService;

    @Test(groups = "functional")
    public void testAutowire() {
        Assert.assertNotNull(queryEvaluator);
    }

    @Test(groups = "functional")
    public void testAccountQuery() {
        DataRequest dataRequest = new DataRequest();
        dataRequest.setAccountIds(Arrays.asList("1", "2", "3"));
        dataRequest.setAttributes(Arrays.asList("companyname", "city", "state"));
        Query query = accountQueryService.generateAccountQuery(DateTimeUtils.convertToStringUTCISO8601(new Date()), 0,
                200, true, dataRequest);

        queryEvaluator.evaluate(attrRepo, query);
    }
}
