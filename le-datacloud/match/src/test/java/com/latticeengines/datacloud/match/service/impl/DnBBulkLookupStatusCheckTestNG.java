package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupStatusChecker;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchGrade;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

public class DnBBulkLookupStatusCheckTestNG extends DataCloudMatchFunctionalTestNGBase {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DnBBulkLookupStatusCheckTestNG.class);

    @Autowired
    private DnBBulkLookupStatusChecker dnbBulkLookupStatusChecker;

    @Autowired
    private DnBBulkLookupDispatcher dnBBulkLookupDispatcher;

    @Test(groups = "dnb", enabled = true)
    public void testStatusCheck() {
        DnBBatchMatchContext context1 = dnBBulkLookupDispatcher.sendRequest(generateInput());
        Assert.assertEquals(context1.getDnbCode(), DnBReturnCode.SUBMITTED);

        List<DnBBatchMatchContext> contexts = new ArrayList<DnBBatchMatchContext>();
        contexts.add(context1);

        contexts = dnbBulkLookupStatusChecker.checkStatus(contexts);
        for (DnBBatchMatchContext context : contexts) {
            Assert.assertEquals(context.getDnbCode(), DnBReturnCode.IN_PROGRESS);
        }

    }

    public static Object[][] getEntityInputData() {
        return new Object[][] {
                { "AMAZON INC", "CHICAGO", "ILLINOIS", "US", "013919572", 7, new DnBMatchGrade("AZZAAZZZFFZ"),
                        "AMAZON INC", "232 E OHIO ST FL 3", "CHICAGO", "IL", "US", "606113217", "(312) 642-5400" },
                { "GOOGLE GERMANY", "HAMBURG", null, "DE", "330465266", 7, new DnBMatchGrade("AZZAZZZZZFZ"),
                        "Google Germany GmbH", "ABC-Str. 19", "Hamburg", "DE", "DE", "20354", "040808179000" } };
    }

    private DnBBatchMatchContext generateInput() {
        DnBBatchMatchContext batchContext = new DnBBatchMatchContext();
        Map<String, DnBMatchContext> contexts = new HashMap<String, DnBMatchContext>();
        for (int i = 0; i < getEntityInputData().length; i++) {
            Object[] record = getEntityInputData()[i];
            String uuid = String.valueOf(i);
            MatchKeyTuple tuple = new MatchKeyTuple();
            tuple.setName((String) record[0]);
            tuple.setCity((String) record[1]);
            tuple.setState((String) record[2]);
            tuple.setCountryCode((String) record[3]);
            DnBMatchContext context = new DnBMatchContext();
            context.setLookupRequestId(uuid);
            context.setInputNameLocation(tuple);
            contexts.put(uuid, context);
        }
        batchContext.setContexts(contexts);
        return batchContext;
    }
}
