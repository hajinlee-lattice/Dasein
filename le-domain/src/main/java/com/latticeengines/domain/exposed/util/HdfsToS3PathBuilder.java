package com.latticeengines.domain.exposed.util;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

public class HdfsToS3PathBuilder {

    private static final String PATH_SEPARATOR = "/";

    private String hddfAnalyticsBaseDir = "/user/s-analytics/customers";
    private String hdfsAnalyticsDir = hddfAnalyticsBaseDir + "/%s";
    private String hdfsEventTableModelDir = hdfsAnalyticsDir + "/models";
    private String hdfsEventTableDataDir = hdfsAnalyticsDir + "/data";

    private String hdfsAtlasDir = "/Pods/%s/Contracts/%s/Tenants/%s/Spaces/Production";
    private String hdfsAtlasDataDir = hdfsAtlasDir + "/Data";
    private String hdfsAtlasMetadataDir = hdfsAtlasDir + "/Metadata";

    private String s3AnalyticsDir = "s3n://%s/%s/analytics";
    private String s3EventTableModelDir = s3AnalyticsDir + "/models";
    private String s3EventTableDataDir = s3AnalyticsDir + "/data";

    private String s3BucketDir = "s3n://%s";
    private String s3AtlasDir = "s3n://%s/%s/atlas";
    private String s3AtlasDataDir = s3AtlasDir + "/Data";
    private String s3AtlasMetadataDir = s3AtlasDir + "/Metadata";

    // Hdfs Atlas
    public String getHdfsAtlasDir(String pod, String tenantId) {
        return String.format(hdfsAtlasDir, pod, tenantId, tenantId);
    }

    public String getHdfsAtlasDataDir(String pod, String tenantId) {
        return String.format(hdfsAtlasDataDir, pod, tenantId, tenantId);
    }

    public String getHdfsAtlasMetadataDir(String pod, String tenantId) {
        return String.format(hdfsAtlasMetadataDir, pod, tenantId, tenantId);
    }

    public String getHdfsAtlasTablesDir(String pod, String tenantId) {
        return getHdfsAtlasDataDir(pod, tenantId) + PATH_SEPARATOR + "Tables";
    }

    public String getHdfsAtlasFilesDir(String pod, String tenantId) {
        return getHdfsAtlasDataDir(pod, tenantId) + PATH_SEPARATOR + "Files";
    }

    public String getHdfsAtlasForTableDir(String pod, String tenantId, String tableName) {
        return getHdfsAtlasTablesDir(pod, tenantId) + PATH_SEPARATOR + tableName;
    }

    public String getHdfsAtlasForFileDir(String pod, String tenantId, String fileName) {
        return getHdfsAtlasFilesDir(pod, tenantId) + PATH_SEPARATOR + fileName;
    }

    // Hdfs Analytics
    public String getHdfsAnalyticsDir(String customer) {
        return String.format(hdfsAnalyticsDir, customer);
    }

    public String getHdfsAnalyticsModelDir(String customer) {
        return String.format(hdfsEventTableModelDir, customer);
    }

    public String getHdfsAnalyticsDataDir(String customer) {
        return String.format(hdfsEventTableDataDir, customer);
    }

    public String getHdfsAnalyticsModelTableDir(String customer, String eventTable) {
        return getHdfsAnalyticsModelDir(customer) + PATH_SEPARATOR + eventTable;
    }

    public String getHdfsAnalyticsDataTableDir(String customer, String eventTable) {
        return getHdfsAnalyticsDataDir(customer) + PATH_SEPARATOR + eventTable;
    }

    public String getHdfsAnalyticsMetaDataTableDir(String customer, String eventTable, String eventColumn) {
        return getHdfsAnalyticsDataDir(customer) + PATH_SEPARATOR + getMetadataTableFolderName(eventTable, eventColumn);
    }

    // S3 Atlas
    public String getS3BucketDir(String s3Bucket) {
        return String.format(s3BucketDir, s3Bucket);
    }

    public String getS3AtlasDir(String s3Bucket, String tenantId) {
        return String.format(s3AtlasDir, s3Bucket, tenantId);
    }

    public String getS3AtlasDataDir(String s3Bucket, String tenantId) {
        return String.format(s3AtlasDataDir, s3Bucket, tenantId);
    }

    public String getS3AtlasMetadataDir(String s3Bucket, String tenantId) {
        return String.format(s3AtlasMetadataDir, s3Bucket, tenantId);
    }

    public String getS3AtlasTablesDir(String s3Bucket, String tenantId) {
        return getS3AtlasDataDir(s3Bucket, tenantId) + PATH_SEPARATOR + "Tables";
    }

    public String getS3AtlasFilesDir(String s3Bucket, String tenantId) {
        return getS3AtlasDataDir(s3Bucket, tenantId) + PATH_SEPARATOR + "Files";
    }

    public String getS3AtlasForTableDir(String s3Bucket, String tenantId, String tableName) {
        return getS3AtlasTablesDir(s3Bucket, tenantId) + PATH_SEPARATOR + tableName;
    }

    public String getS3AtlasForFileDir(String s3Bucket, String tenantId, String fileName) {
        return getS3AtlasFilesDir(s3Bucket, tenantId) + PATH_SEPARATOR + fileName;
    }

    // S3 Analytics
    public String getS3AnalyticsDir(String s3Bucket, String tenantId) {
        return String.format(s3AnalyticsDir, s3Bucket, tenantId);
    }

    public String getS3AnalyticsModelDir(String s3Bucket, String tenantId) {
        return String.format(s3EventTableModelDir, s3Bucket, tenantId);
    }

    public String getS3AnalyticsDataDir(String s3Bucket, String tenantId) {
        return String.format(s3EventTableDataDir, s3Bucket, tenantId);
    }

    public String getS3AnalyticsModelTableDir(String s3Bucket, String tenantId, String eventTable) {
        return getS3AnalyticsModelDir(s3Bucket, tenantId) + PATH_SEPARATOR + eventTable;
    }

    public String getS3AnalyticsDataTableDir(String s3Bucket, String tenantId, String eventTable) {
        return getS3AnalyticsDataDir(s3Bucket, tenantId) + PATH_SEPARATOR + eventTable;
    }

    public String getS3AnalyticsMetaDataTableDir(String s3Bucket, String tenantId, String eventTable,
            String eventColumn) {
        return getS3AnalyticsDataDir(s3Bucket, tenantId) + PATH_SEPARATOR
                + getMetadataTableFolderName(eventTable, eventColumn);
    }

    // Converters
    public String convertAtlasTableDir(String inputTableDir, String pod, String tenantId, String s3Bucket) {
        inputTableDir = getFullPath(inputTableDir);
        StringBuilder builder = new StringBuilder();
        String hdfsTablesDir = getHdfsAtlasTablesDir(pod, tenantId);
        if (inputTableDir.startsWith(hdfsTablesDir)) {
            return builder.append(getS3AtlasTablesDir(s3Bucket, tenantId))
                    .append(inputTableDir.substring(hdfsTablesDir.length())).toString();
        }
        String lastDir = FilenameUtils.getName(inputTableDir);
        return builder.append(getS3AtlasTablesDir(s3Bucket, tenantId)).append(PATH_SEPARATOR).append(lastDir)
                .toString();
    }

    public String getFullPath(String dir) {
        String fileName = FilenameUtils.getName(dir);
        if (fileName.startsWith("*.") || dir.endsWith("/")) {
            dir = FilenameUtils.getFullPathNoEndSeparator(dir);
        }
        return dir;
    }

    public String convertAtlasFile(String inputFileDir, String pod, String tenantId, String s3Bucket) {
        StringBuilder builder = new StringBuilder();
        String hdfsFilesDir = getHdfsAtlasFilesDir(pod, tenantId);
        if (inputFileDir.startsWith(hdfsFilesDir)) {
            return builder.append(getS3AtlasFilesDir(s3Bucket, tenantId))
                    .append(inputFileDir.substring(hdfsFilesDir.length())).toString();
        }
        String fileName = FilenameUtils.getName(inputFileDir);
        return builder.append(getS3AtlasFilesDir(s3Bucket, tenantId)).append(PATH_SEPARATOR).append(fileName)
                .toString();
    }

    public String convertAtlasMetadata(String inputFileDir, String pod, String tenantId, String s3Bucket) {
        StringBuilder builder = new StringBuilder();
        String hdfsMetadataDir = getHdfsAtlasMetadataDir(pod, tenantId);
        if (inputFileDir.startsWith(hdfsMetadataDir)) {
            return builder.append(getS3AtlasMetadataDir(s3Bucket, tenantId))
                    .append(inputFileDir.substring(hdfsMetadataDir.length())).toString();
        }
        String fileName = FilenameUtils.getName(inputFileDir);
        return builder.append(getS3AtlasMetadataDir(s3Bucket, tenantId)).append(PATH_SEPARATOR).append(fileName)
                .toString();
    }

    public String exploreS3FilePath(String inputFileDir, String pod, String customer, String tenantId,
            String s3Bucket) {
        StringBuilder builder = new StringBuilder();
        String hdfsFilesDir = getHdfsAnalyticsDir(customer);
        if (inputFileDir.startsWith(hdfsFilesDir)) {
            return builder.append(getS3AnalyticsDir(s3Bucket, tenantId))
                    .append(inputFileDir.substring(hdfsFilesDir.length())).toString();
        }

        hdfsFilesDir = getHdfsAtlasDir(pod, tenantId);
        if (inputFileDir.startsWith(hdfsFilesDir)) {
            return builder.append(getS3AtlasDir(s3Bucket, tenantId))
                    .append(inputFileDir.substring(hdfsFilesDir.length())).toString();
        }
        return inputFileDir;
    }

    public String toParentDir(String dir) {
        return StringUtils.substringBeforeLast(dir, PATH_SEPARATOR);
    }

    private String getMetadataTableFolderName(String eventTable, String eventColumn) {
        return String.format("%s-%s-Metadata", eventTable.replaceAll("[^A-Za-z0-9_-]", "_"),
                eventColumn.replaceAll("[^A-Za-z0-9_-]", "_"));
    }
}
