package com.latticeengines.domain.exposed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class HdfsToS3PathBuilderUnitTestNG {

    @Test(groups = "unit")
    public void getAtlasBaseDir() {
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
    public void getAtlasTablesDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.getHdfsAtlasForTableDir("pod2", "tenantId2", "table2"),
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/table2");
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
    public void convertAtlasTableDir() {
        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder();
        Assert.assertEquals(builder.convertAtlasTableDir(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/table2", "pod2",
                "tenantId2", "bucket2"), "s3n://bucket2/tenantId2/atlas/Data/Tables/table2");
        Assert.assertEquals(builder.convertAtlasTableDir(
                "hdfs://QACLUSTER2/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/table2/*.avro",
                "pod2",
                "tenantId2", "bucket2"), "s3n://bucket2/tenantId2/atlas/Data/Tables/table2");
        Assert.assertEquals(builder.convertAtlasTableDir(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/table2/", "pod2",
                "tenantId2", "bucket2"), "s3n://bucket2/tenantId2/atlas/Data/Tables/table2");
        Assert.assertEquals(builder.convertAtlasTableDir(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/table2/*.avro", "pod2",
                "tenantId2", "bucket2"), "s3n://bucket2/tenantId2/atlas/Data/Tables/table2");

        Assert.assertEquals(builder.convertAtlasTableDir(
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Data/Tables/File/table2/table3/*.avro",
                "pod2", "tenantId2", "bucket2"), "s3n://bucket2/tenantId2/atlas/Data/Tables/File/table2/table3");
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
                "/Pods/pod2/Contracts/tenantId2/Tenants/tenantId2/Spaces/Production/Metadata/Export/file2.csv",
                "pod2", "tenantId2", "bucket2"), "s3n://bucket2/tenantId2/atlas/Metadata/Export/file2.csv");
    }

}
