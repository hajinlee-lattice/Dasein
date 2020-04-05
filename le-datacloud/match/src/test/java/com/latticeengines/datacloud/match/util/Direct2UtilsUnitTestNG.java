package com.latticeengines.datacloud.match.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

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

public class Direct2UtilsUnitTestNG {

    @Test
    public void parseIdentityLocatingResult() {
        DnBMatchContext context = new DnBMatchContext();
        String response = readMockResponse("trxn_1");
        Direct2Utils.parseJsonResponse(response, context, DnBAPIType.REALTIME_ENTITY);
        Assert.assertTrue(CollectionUtils.isNotEmpty(context.getCandidates()));
        for (DnBMatchCandidate candidate: context.getCandidates()) {
            Assert.assertNotNull(candidate.getDuns());
            NameLocation nameLocation = candidate.getNameLocation();
            Assert.assertNotNull(nameLocation);
            DnBMatchInsight matchInsight = candidate.getMatchInsight();
            Assert.assertNotNull(matchInsight);
        }
    }


    private String readMockResponse(String name) {
        try {
            InputStream is = new ClassPathResource("direct2/" + name + ".json").getInputStream();
            return IOUtils.toString(is, Charset.defaultCharset());
        } catch (IOException e) {
            Assert.fail("Failed to read mock response " + name, e);
            return null;
        }
    }

}
