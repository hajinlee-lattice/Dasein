package com.latticeengines.domain.exposed.datacloud.customer;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;

public class CustomerReportUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        CustomerReport report = new CustomerReport();
        String id = UUID.randomUUID().toString();
        report.setId(id);
        report.setType(CustomerReportType.LOOkUP);
        report.setComment("this is test!");
        report.setCreatedTime(new Date());
        report.setReportedByUser("penglong.liu@lattice-engines.com");
        report.setSuggestedValue("test");

        IncorrectLookupReproduceDetail lookupDetail = new IncorrectLookupReproduceDetail();
        Map<String, String> inputKeys = new HashMap<>();
        Map<String, String> matchedKeys = new HashMap<>();
        inputKeys.put(MatchKey.Country.toString(), "United States");
        matchedKeys.put(MatchKey.City.toString(), "New York");
        lookupDetail.setInputKeys(inputKeys);
        lookupDetail.setMatchedKeys(matchedKeys);
        report.setReproduceDetail(lookupDetail);

        String json = JsonUtils.serialize(report);
        CustomerReport equalReport = JsonUtils.deserialize(json, CustomerReport.class);

        Assert.assertEquals(id, equalReport.getId());
        Assert.assertEquals(CustomerReportType.LOOkUP, equalReport.getType());
        Assert.assertTrue(equalReport.getReproduceDetail() instanceof IncorrectLookupReproduceDetail);

        IncorrectMatchedAttributeReproduceDetail matchDetail = new IncorrectMatchedAttributeReproduceDetail();
        matchDetail.setInputKeys(inputKeys);
        matchDetail.setMatchedKeys(matchedKeys);
        matchDetail.setAttribute("Country");
        matchDetail.setMatchedValue("United States");
        report.setReproduceDetail(matchDetail);
        String json1 = JsonUtils.serialize(report);
        CustomerReport equalReport1 = JsonUtils.deserialize(json1, CustomerReport.class);
        Assert.assertTrue(equalReport1.getReproduceDetail() instanceof IncorrectMatchedAttributeReproduceDetail);
    }
}
