package com.latticeengines.domain.exposed.eai;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.route.SftpToHdfsRouteConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;

public class EaiSerUnitTest {
    @Test(groups = "unit")
    public void testSer() {
        HdfsToS3Configuration conf = new HdfsToS3Configuration();
        conf.setExportInputPath("aaa");
        conf.setS3Prefix("bbb");
        String s = JsonUtils.serialize(conf);
        EaiJobConfiguration jobConfig = JsonUtils.deserialize(s, EaiJobConfiguration.class);
        assertEquals(((HdfsToS3Configuration) jobConfig).getExportInputPath(), "aaa");
        assertEquals(((HdfsToS3Configuration) jobConfig).getS3Prefix(), "bbb");
    }

    @Test(groups = "unit")
    public void testSer2() {
        ExportConfiguration conf = new ExportConfiguration();
        conf.setExportDestination(ExportDestination.FILE);
        conf.setExportInputPath("aaa");
        conf.setExportTargetPath("bbb");
        String s = JsonUtils.serialize(conf);
        EaiJobConfiguration jobConfig = JsonUtils.deserialize(s, EaiJobConfiguration.class);
        assertEquals(((ExportConfiguration) jobConfig).getExportInputPath(), "aaa");
        assertEquals(((ExportConfiguration) jobConfig).getExportTargetPath(), "bbb");
    }

    @Test(groups = "unit")
    public void testSer3() {
        SftpToHdfsRouteConfiguration conf = new SftpToHdfsRouteConfiguration();
        conf.setFileName("aaa");
        conf.setSftpDir("bbb");
        String s = JsonUtils.serialize(conf);
        EaiJobConfiguration jobConfig = JsonUtils.deserialize(s, EaiJobConfiguration.class);
        assertEquals(((SftpToHdfsRouteConfiguration) jobConfig).getFileName(), "aaa");
        assertEquals(((SftpToHdfsRouteConfiguration) jobConfig).getSftpDir(), "bbb");
    }

    @Test(groups = "unit")
    public void testSer4() {
        HdfsToRedshiftConfiguration conf = new HdfsToRedshiftConfiguration();
        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setTableName("aaa");
        conf.setRedshiftTableConfiguration(redshiftTableConfig);
        redshiftTableConfig.setJsonPathPrefix("bbb");
        String s = JsonUtils.serialize(conf);
        System.out.println(s);
        EaiJobConfiguration jobConfig = JsonUtils.deserialize(s, EaiJobConfiguration.class);
        assertEquals(((HdfsToRedshiftConfiguration) jobConfig).getRedshiftTableConfiguration().getTableName(), "aaa");
        assertEquals(((HdfsToRedshiftConfiguration) jobConfig).getRedshiftTableConfiguration().getJsonPathPrefix(), "bbb");
    }
}
