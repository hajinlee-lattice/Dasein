package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestMatchInputService;
import com.latticeengines.datacloud.match.testframework.TestMatchInputUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component
public class RealTimeMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Autowired
    private List<RealTimeMatchService> realTimeMatchServiceList;

    @Autowired
    private TestMatchInputService testMatchInputService;

    @Test(groups = "functional")
    public void testSimpleMatch() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = getMatchService(input).match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    @Test(groups = "functional")
    public void testDuns() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA", "12345" },
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA", 12345 }
        };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, true);
        MatchOutput output = getMatchService(input).match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    @Test(groups = "functional")
    public void testMatchEnrichment() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        input.setPredefinedSelection(null);
        input.setCustomSelection(testMatchInputService.enrichmentSelection());
        MatchOutput output = getMatchService(input).match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    @Test(groups = "functional")
    public void testIsPublicDomain() {
        Object[][] data = new Object[][] { { 123, "my@gmail.com", null, null, null, null } };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = getMatchService(input).match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);

        Integer pos = output.getOutputFields().indexOf("IsPublicDomain");
        Assert.assertTrue(Boolean.TRUE.equals(output.getResult().get(0).getOutput().get(pos)));
    }

    @Test(groups = "functional")
    public void testExcludePublicDomain() {
        Object[][] data = new Object[][] { { 123, "my@gmail.com", null, null, null, null } };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        input.setExcludePublicDomains(true);
        MatchOutput output = getMatchService(input).match(input);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 0);
    }

    private RealTimeMatchService getMatchService(MatchInput input) {
        for (RealTimeMatchService handler : realTimeMatchServiceList) {
            if (handler.accept(input.getDataCloudVersion())) {
                return handler;
            }
        }
        throw new LedpException(LedpCode.LEDP_25021, new String[] { input.getDataCloudVersion() });
    }
}
