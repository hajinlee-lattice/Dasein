package com.latticeengines.domain.exposed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class HdfsToS3PathBuilderUnitTestNG {

    @Test(groups = "unit")
    public void getHdfsAnalyticsDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getHdfsAnalyticsDir("tenantId2"), "/user/s-analytics/customers/tenantId2");
    }

    @Test(groups = "unit")
    public void getHdfsAtlasDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getHdfsAtlasDir("pod2", "tenantId2"),
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production");
    }

    @Test(groups = "unit")
    public void getHdfsAtlasDataDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getHdfsAtlasDataDir("pod2", "tenantId2"),
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data");
    }

    @Test(groups = "unit")
    public void getHdfsAtlasMetadataDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getHdfsAtlasMetadataDir("pod2", "tenantId2"),
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Metadata");
    }

    @Test(groups = "unit")
    public void getHdfsAtlasForTableDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getHdfsAtlasForTableDir("pod2", "tenantId2", "table2"),
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/table2");
    }

    @Test(groups = "unit")
    public void getS3BucketDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getS3BucketDir("bucket2"), "s3n://bucket2");
    }

    @Test(groups = "unit")
    public void getS3AtlasDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getS3AtlasDir("bucket2", "tenantId2"), "s3n://bucket2/tenantId2/atlas");
    }

    @Test(groups = "unit")
    public void getS3AtlasForTableDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getS3AtlasForTableDir("bucket2", "tenantId2", "table2"),
                "s3n://bucket2/tenantId2/atlas/Data/Tables/table2");
    }

    @Test(groups = "unit")
    public void getS3AtlasForFileDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getS3AtlasForFileDir("bucket2", "tenantId2", "file2"),
                "s3n://bucket2/tenantId2/atlas/Data/Files/file2");
    }

    @Test(groups = "unit")
    public void getS3AtlasForMetadataDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getS3AtlasMetadataDir("bucket2", "tenantId2"),
                "s3n://bucket2/tenantId2/atlas/Metadata");
    }

    @Test(groups = "unit")
    public void getS3AnalyticsDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getS3AnalyticsDir("bucket2", "tenantId2"), "s3n://bucket2/tenantId2/analytics");
    }

    @Test(groups = "unit")
    public void getS3AnalyticsModelDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getS3AnalyticsModelDir("bucket2", "tenantId2"),
                "s3n://bucket2/tenantId2/analytics/models");
    }

    @Test(groups = "unit")
    public void getS3AnalyticsDataDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getS3AnalyticsDataDir("bucket2", "tenantId2"),
                "s3n://bucket2/tenantId2/analytics/data");
    }

    @Test(groups = "unit")
    public void getS3AnalyticsModelTableDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getS3AnalyticsModelTableDir("bucket2", "tenantId2", "table2"),
                "s3n://bucket2/tenantId2/analytics/models/table2");
    }

    @Test(groups = "unit")
    public void getS3AnalyticsDataTableDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getS3AnalyticsDataTableDir("bucket2", "tenantId2", "table2"),
                "s3n://bucket2/tenantId2/analytics/data/table2");
    }

    @Test(groups = "unit")
    public void getS3AnalyticsMetaDataTableDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getS3AnalyticsMetaDataTableDir("bucket2", "tenantId2", "table2", "Event"),
                "s3n://bucket2/tenantId2/analytics/data/table2-Event-Metadata");
    }

    @Test(groups = "unit")
    public void testConvertS3CampaignExportDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        String csvExportFile = "/Pods/Default/Contracts/JLM_1557521565766/Tenants/JLM_1557521565766/Spaces/Production/Data/Files/Exports/FILE_SYSTEM/AWS_S3/Lattice_S3/play__5d328d85-51b6-4b97-a542-c7b866ee986f/launch__9673cea8-fe65-4d57-bc47-7a49d31efb1e/Recommendations_2019-05-10_20-59-40_UTC.csv";
        String result = builder.convertS3CampaignExportDir(csvExportFile, "bucket2", "tenantId2", "playId", "playName");
        Assert.assertTrue(result.startsWith("s3n://bucket2/dropfolder/tenantId2/export/campaigns/playName-playId-"));
        Assert.assertTrue(result.endsWith(".csv"));

        String jsonExportFile = "/Pods/Default/Contracts/JLM_1557521565766/Tenants/JLM_1557521565766/Spaces/Production/Data/Files/Exports/FILE_SYSTEM/AWS_S3/Lattice_S3/play__5d328d85-51b6-4b97-a542-c7b866ee986f/launch__9673cea8-fe65-4d57-bc47-7a49d31efb1e/Recommendations_2019-05-10_20-59-40_UTC.json";
        result = builder.convertS3CampaignExportDir(jsonExportFile, "bucket2", "tenantId2", "playId", "playName");
        Assert.assertTrue(result.startsWith("s3n://bucket2/dropfolder/tenantId2/export/campaigns/playName-playId-"));
        Assert.assertTrue(result.endsWith(".json"));
    }

    @Test(groups = "unit")
    public void convertAtlasTableDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        builder.setProtocol("s3a");

        Assert.assertEquals(builder.convertAtlasTableDir(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/table2", "pod2",
                "tenantId2", "bucket2"), "s3a://bucket2/tenantId2/atlas/Data/Tables/table2");
        Assert.assertEquals(builder.convertAtlasTableDir(
                "hdfs://QACLUSTER2/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/table2/*.avro",
                "pod2", "tenantId2", "bucket2"), "s3a://bucket2/tenantId2/atlas/Data/Tables/table2");
        Assert.assertEquals(builder.convertAtlasTableDir(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/table2/", "pod2",
                "tenantId2", "bucket2"), "s3a://bucket2/tenantId2/atlas/Data/Tables/table2");
        Assert.assertEquals(builder.convertAtlasTableDir(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/table2/*.avro", "pod2",
                "tenantId2", "bucket2"), "s3a://bucket2/tenantId2/atlas/Data/Tables/table2");

        Assert.assertEquals(builder.convertAtlasTableDir(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/File/table2/table3/*.avro",
                "pod2", "tenantId2", "bucket2"), "s3a://bucket2/tenantId2/atlas/Data/Tables/File/table2/table3");

        Assert.assertEquals(builder.convertAtlasTableDir(
                "/Pods/QA/Contracts/QA_LPI_Auto_Refine/Tenants/QA_LPI_Auto_Refine/Spaces/Production/Data/Tables/File/SourceFile_file_1477293584451_csv/Extracts/2016-10-24-03-20-35",
                "QA", "LPI_QA_Auto_ReBuild2", "bucket2"),
                "s3a://bucket2/QA_LPI_Auto_Refine/atlas/Data/Tables/File/SourceFile_file_1477293584451_csv/Extracts/2016-10-24-03-20-35");

        String tgtDir = builder.convertAtlasTableDir(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/table2", "pod2",
                "tenantId2", "bucket2");
        String prefix = tgtDir.substring(tgtDir.indexOf("bucket2") + "bucket2".length() + 1);
        Assert.assertEquals(prefix, "tenantId2/atlas/Data/Tables/table2");
    }

    @Test(groups = "unit")
    public void convertAtlasFile() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.convertAtlasFile(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Files/file2.csv", "pod2",
                "tenantId2", "bucket2"), "s3n://bucket2/tenantId2/atlas/Data/Files/file2.csv");
        Assert.assertEquals(builder.convertAtlasFile(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Files/Export/file2.csv",
                "pod2", "tenantId2", "bucket2"), "s3n://bucket2/tenantId2/atlas/Data/Files/Export/file2.csv");
    }

    @Test(groups = "unit")
    public void convertAtlasMetadataDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.convertAtlasMetadata(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Metadata/file2.csv", "pod2",
                "tenantId2", "bucket2"), "s3n://bucket2/tenantId2/atlas/Metadata/file2.csv");
        Assert.assertEquals(builder.convertAtlasMetadata(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Metadata/Export/file2.csv", "pod2",
                "tenantId2", "bucket2"), "s3n://bucket2/tenantId2/atlas/Metadata/Export/file2.csv");
    }

    @Test(groups = "unit")
    public void exploreS3FilePath() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.exploreS3FilePath(
                "/user/s-analytics/customers/tenantId2.tenantId2.Production/data/table2/samples/file2.csv", "bucket2"),
                "s3n://bucket2/tenantId2/analytics/data/table2/samples/file2.csv");
        Assert.assertEquals(builder.exploreS3FilePath(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Metadata/Export/file2.csv",
                "bucket2"), "s3n://bucket2/tenantId2/atlas/Metadata/Export/file2.csv");
        Assert.assertEquals(builder.exploreS3FilePath(
                "/Pods/pod2//Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Metadata/Export", "bucket2"),
                "s3n://bucket2/tenantId2/atlas/Metadata/Export");
        Assert.assertEquals(builder.exploreS3FilePath(
                "hdfs://localhost:50070/Pods/pod2//Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Metadata/Export",
                "bucket2"), "s3n://bucket2/tenantId2/atlas/Metadata/Export");
    }

    @Test(groups = "unit")
    public void getCustomerFromHdfsPath() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(
                builder.getCustomerFromHdfsPath(
                        "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Metadata/Export/file2.csv"),
                "tenantId2.tenantId2.Production");
        Assert.assertEquals(
                builder.getCustomerFromHdfsPath(
                        "/user/s-analytics/customers/tenantId2.tenantId2.Production/data/table2/samples/file2.csv"),
                "tenantId2.tenantId2.Production");
    }

}
