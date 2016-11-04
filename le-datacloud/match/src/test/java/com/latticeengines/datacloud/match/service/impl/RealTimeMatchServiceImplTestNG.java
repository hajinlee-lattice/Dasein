package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestMatchInputService;
import com.latticeengines.datacloud.match.testframework.TestMatchInputUtils;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Component
public class RealTimeMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Autowired
    private RealTimeMatchService realTimeMatchService;

    @Autowired
    private TestMatchInputService testMatchInputService;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Test(groups = "functional")
    public void testSimpleMatch() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    @Test(groups = "functional")
    public void testSimpleRealTimeBulkMatch() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };
        List<MatchInput> inputs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
            inputs.add(input);
        }
        BulkMatchInput bulkMatchInput = new BulkMatchInput();
        bulkMatchInput.setInputList(inputs);
        BulkMatchOutput bulkMatchOutput = realTimeMatchService.matchBulk(bulkMatchInput);
        Assert.assertNotNull(bulkMatchOutput);
        Assert.assertEquals(bulkMatchOutput.getOutputList().size(), 50);

        MatchOutput output = bulkMatchOutput.getOutputList().get(0);
        Assert.assertNotNull(output.getResult());
        Assert.assertEquals(output.getResult().size(), 1);
    }

    @Test(groups = "functional")
    public void testSimpleMatchAccountMaster() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        DataCloudVersion version = versionEntityMgr.latestApprovedForMajorVersion("2.0");
        input.setDataCloudVersion(version.getVersion());
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);

    }

    @Test(groups = "functional")
    public void testSimpleRealTimeBulkMatchAccountMaster() {
        DataCloudVersion version = versionEntityMgr.latestApprovedForMajorVersion("2.0");
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };
        List<MatchInput> inputs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
            input.setDataCloudVersion(version.getVersion());
            inputs.add(input);
        }
        BulkMatchInput bulkMatchInput = new BulkMatchInput();
        bulkMatchInput.setInputList(inputs);
        BulkMatchOutput bulkMatchOutput = realTimeMatchService.matchBulk(bulkMatchInput);
        Assert.assertNotNull(bulkMatchOutput);
        Assert.assertEquals(bulkMatchOutput.getOutputList().size(), 50);

        MatchOutput output = bulkMatchOutput.getOutputList().get(0);
        Assert.assertNotNull(output.getResult());
        Assert.assertEquals(output.getResult().size(), 1);
        List<Object> objs = output.getResult().get(0).getOutput();
        int notNulls = 0;
        for (Object obj: objs) {
            if (obj != null) {
                notNulls++;
            }
        }
        Assert.assertTrue(notNulls > 5);
    }

    @Test(groups = "functional")
    public void testDuns() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "12345" },
                { 123, "chevron.com", 12345 }
        };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, new String[]{ "ID", "Domain", "DUNS" });
        MatchOutput output = realTimeMatchService.match(input);
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
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    @Test(groups = "functional")
    public void testIsPublicDomain() {
        Object[][] data = new Object[][] { { 123, "my@gmail.com", null, null, null, null } };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = realTimeMatchService.match(input);
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
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 0);
    }

    @Test(groups = "functional")
    public void testTwoStepMatching() {
        Object[][] data = new Object[][] {
                { 1, "chevron.com" },
                { 2, "my@gmail.com" }
        };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, new String[]{"ID", "Domain"});
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 2);
        Assert.assertEquals(output.getOutputFields().size(), 1);
        Assert.assertEquals(output.getOutputFields().get(0), MatchConstants.LID_FIELD);

        String latticeAccountId = (String) output.getResult().get(0).getOutput().get(0);
        Assert.assertNotNull(latticeAccountId);
        Assert.assertNull(output.getResult().get(1).getOutput().get(0));

        data = new Object[][] {
                { 1, latticeAccountId, "chevron.com" },
                { 2, null, "my@gmail.com" }
        };
        input = TestMatchInputUtils.prepareSimpleMatchInput(data, new String[]{ "ID", MatchConstants.LID_FIELD, "Domain" });
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        input.setFetchOnly(true);
        output = realTimeMatchService.match(input);

        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 2);
        Assert.assertEquals(output.getStatistics().getRowsMatched(), new Integer(2));
        output.setMetadata(null);
        System.out.println(JsonUtils.serialize(output));
    }
}
