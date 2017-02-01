package com.latticeengines.eai.yarn.runtime;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.route.HdfsToS3Configuration;

public class EaiSerUnitTest {
    @Test(groups = "unit")
    public void testSer() {
        HdfsToS3Configuration conf = new HdfsToS3Configuration();
        conf.setHdfsPath("aaa");
        conf.setS3Prefix("bbb");
        String s = JsonUtils.serialize(conf);
        EaiJobConfiguration jobConfig = JsonUtils.deserialize(s, EaiJobConfiguration.class);
        assertEquals(((HdfsToS3Configuration) jobConfig).getHdfsPath(), "aaa");
        assertEquals(((HdfsToS3Configuration) jobConfig).getS3Prefix(), "bbb");
    }
}
