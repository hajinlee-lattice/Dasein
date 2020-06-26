package com.latticeengines.datacloud.match.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ClassPathResource;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBAPIType;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchInsight;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class DirectPlusUtilsUnitTestNG {

    @Test
    public void parseIdentityLocatingResult() {
        DnBMatchContext context = new DnBMatchContext();
        String response = readMockResponse("cleanseMatch");
        DirectPlusUtils.parseJsonResponse(response, context, DnBAPIType.REALTIME_ENTITY);
        Assert.assertTrue(CollectionUtils.isNotEmpty(context.getCandidates()));
        for (DnBMatchCandidate candidate: context.getCandidates()) {
            Assert.assertNotNull(candidate.getDuns());
            NameLocation nameLocation = candidate.getNameLocation();
            Assert.assertNotNull(nameLocation);
            if ("060902413".equals(candidate.getDuns())) {
                Assert.assertNotNull(nameLocation.getStreet());
                Assert.assertNotEquals(nameLocation.getStreet(), "null");
                Assert.assertNotNull(nameLocation.getState());
                Assert.assertNotEquals(nameLocation.getState(), "null");
                Assert.assertNotNull(nameLocation.getPhoneNumber());
                Assert.assertNotEquals(nameLocation.getPhoneNumber(), "null");
            }
            Assert.assertNotNull(candidate.getOperatingStatus());
            System.out.println(candidate.getOperatingStatus());
            DnBMatchInsight matchInsight = candidate.getMatchInsight();
            Assert.assertNotNull(matchInsight);
            Assert.assertNotNull(matchInsight.getConfidenceCode());
            Assert.assertNotNull(matchInsight.getNameMatchScore());
        }
    }

    @Test
    public void parseDataBlock() {
        String response = readMockResponse("compinfo");
        Map<String, Object> result = DirectPlusUtils.parseDataBlock(response);
        Assert.assertNotNull(result.get("TradeStyleName"));
    }

    private String readMockResponse(String name) {
        try {
            InputStream is = new ClassPathResource("direct_plus/" + name + ".json").getInputStream();
            return IOUtils.toString(is, Charset.defaultCharset());
        } catch (IOException e) {
            Assert.fail("Failed to read mock response " + name, e);
            return null;
        }
    }

}
