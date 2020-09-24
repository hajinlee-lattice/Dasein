package com.latticeengines.domain.exposed.dcp;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;

public class SourceUnitTestNG {

    @Test(groups = "unit")
    public void testGetRelativePath() {
        Source source = new Source();
        String relativePath  = "Projects/Project_f1iaf08m/Sources/Source_07ckqmax/";
        source.setSourceFullPath(String.format("%s/%s/%s", "lattice-engines-dev", "dropfolder/abcdefgh", relativePath));
        source.setDropFullPath(source.getSourceFullPath() + "drop/");
        Assert.assertEquals(source.getRelativePathUnderDropfolder(), relativePath);
    }

    @Test
    public void testPurposeOfUse() {
        PurposeOfUse purposeOfUse = new PurposeOfUse();
        purposeOfUse.setDomain(DataDomain.EnterpriseMasterData);
        purposeOfUse.setRecordType(DataRecordType.MasterData);
        System.out.println(JsonUtils.pprint(purposeOfUse));
        String x = JsonUtils.serialize(purposeOfUse);
        System.out.println(x);
        Assert.assertTrue(x.contains("D&B for Enterprise Master Data"));
        PurposeOfUse y = JsonUtils.deserialize(x, PurposeOfUse.class);
        Assert.assertEquals(y.getDomain(), DataDomain.EnterpriseMasterData);
    }

}
