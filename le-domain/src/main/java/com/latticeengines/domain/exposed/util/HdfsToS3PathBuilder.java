package com.latticeengines.domain.exposed.util;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;

public class HdfsToS3PathBuilder {

    private static final Logger log = LoggerFactory.getLogger(HdfsToS3PathBuilder.class);

    // use s3a whenever possible, e.g. in emr
    private String protocol = "s3n";

    private static final String PATH_SEPARATOR = "/";
    private static final String PROTOCOL_SEPARATOR = ":/";
    private static final String FILE_NAME_SEPARATOR = "-";

    private String hdfsAnalyticsBaseDir = "/user/s-analytics/customers";
    private String hdfsAnalyticsDir = hdfsAnalyticsBaseDir + "/%s";
    private String hdfsEventTableModelDir = hdfsAnalyticsDir + "/models";
    private String hdfsEventTableDataDir = hdfsAnalyticsDir + "/data";

    private String hdfsAtlasDir = "/Pods/%s/Contracts/%s/Tenants/%s/Spaces/Production";
    private String hdfsAtlasDataDir = hdfsAtlasDir + "/Data";
    private String hdfsAtlasMetadataDir = hdfsAtlasDir + "/Metadata";

    private String s3AnalyticsDir = PROTOCOL_SEPARATOR + "/%s/%s/analytics";
    private String s3EventTableModelDir = s3AnalyticsDir + "/models";
    private String s3EventTableDataDir = s3AnalyticsDir + "/data";

    private String s3BucketDir = PROTOCOL_SEPARATOR + "/%s";
    private String s3AtlasPrefix = "%s/atlas";
    private String s3AtlasDir = PROTOCOL_SEPARATOR + "/%s/" + s3AtlasPrefix;
    private String s3AtlasIntegrationDir = PROTOCOL_SEPARATOR + "/%s/dropfolder/%s/atlas";
    private String s3AtlasDataDir = s3AtlasDir + "/Data";
    private String s3AtlasDataPrefix = s3AtlasPrefix + "/Data";
    private String s3AtlasTablePrefix = s3AtlasDataPrefix + "/Tables";
    private String s3AtlasExportFileDir = s3AtlasDataDir + "/Files/Export";
    private String s3AtlasMetadataDir = s3AtlasDir + "/Metadata";
    private String s3ExportDir = s3BucketDir + "/dropfolder/%s/export";


    public HdfsToS3PathBuilder() {
    }

    public HdfsToS3PathBuilder(Boolean useEmr) {
        this.protocol = Boolean.TRUE.equals(useEmr) ? "s3a" : "s3n";
    }

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

    public String getHdfsAtlasFileExportDir(String pod, String tenantId) {
        return getHdfsAtlasFilesDir(pod, tenantId) + PATH_SEPARATOR + "Exports";
    }

    public String getHdfsAtlasTablesDir(String pod, String tenantId) {
        return getHdfsAtlasDataDir(pod, tenantId) + PATH_SEPARATOR + "Tables";
    }

    public String getHdfsAtlasTableSchemasDir(String pod, String tenantId) {
        return getHdfsAtlasDataDir(pod, tenantId) + PATH_SEPARATOR + "TableSchemas";
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
    public String getHdfsAnalyticsBaseDir() {
        return hdfsAnalyticsBaseDir;
    }

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
        return String.format(protocol + s3BucketDir, s3Bucket);
    }

    public String getS3AtlasDir(String s3Bucket, String tenantId) {
        return String.format(protocol + s3AtlasDir, s3Bucket, tenantId);
    }

    public String getS3AtlasDataDir(String s3Bucket, String tenantId) {
        return String.format(protocol + s3AtlasDataDir, s3Bucket, tenantId);
    }

    public String getS3AtlasDataPrefix(String s3Bucket, String tenantId) {
        return String.format(s3AtlasDataPrefix, tenantId);
    }

    public String getS3AtlasExportFileDir(String s3Bucket, String tenantId) {
        return String.format(protocol + s3AtlasExportFileDir, s3Bucket, tenantId);
    }

    public String getS3AtlasIntegrationsDir(String s3Bucket, String dropboxName) {
        return String.format(protocol + s3AtlasIntegrationDir, s3Bucket, dropboxName);
    }

    public String getS3ExportDir(String s3Bucket, String dropboxName) {
        return String.format(protocol + s3ExportDir, s3Bucket, dropboxName);
    }

    public String getS3CampaignExportDir(String s3Bucket, String dropboxName) {
        return getS3ExportDir(s3Bucket, dropboxName) + PATH_SEPARATOR + "campaigns";
    }

    public String getS3AtlasMetadataDir(String s3Bucket, String tenantId) {
        return String.format(protocol + s3AtlasMetadataDir, s3Bucket, tenantId);
    }

    public String getS3AtlasFileExportsDir(String s3Bucket, String dropboxName) {
        return getS3AtlasIntegrationsDir(s3Bucket, dropboxName) + PATH_SEPARATOR + "Data" + PATH_SEPARATOR + "Files"
                + PATH_SEPARATOR + "Exports";
    }

    public String getS3AtlasTablesDir(String s3Bucket, String tenantId) {
        return getS3AtlasDataDir(s3Bucket, tenantId) + PATH_SEPARATOR + "Tables";
    }

    public String getS3AtlasTableSchemasDir(String s3Bucket, String tenantId) {
        return getS3AtlasDataDir(s3Bucket, tenantId) + PATH_SEPARATOR + "TableSchemas";
    }

    public String getS3AtlasFilesDir(String s3Bucket, String tenantId) {
        return getS3AtlasDataDir(s3Bucket, tenantId) + PATH_SEPARATOR + "Files";
    }

    public String getS3AtlasForTableDir(String s3Bucket, String tenantId, String tableName) {
        return getS3AtlasTablesDir(s3Bucket, tenantId) + PATH_SEPARATOR + tableName;
    }

    public String getS3AtlasTablePrefix(String tenantId, String tableName) {
        return String.format(s3AtlasTablePrefix, tenantId) + PATH_SEPARATOR + tableName;
    }

    public String getS3AtlasForFileDir(String s3Bucket, String tenantId, String fileName) {
        return getS3AtlasFilesDir(s3Bucket, tenantId) + PATH_SEPARATOR + fileName;
    }

    // S3 Analytics
    public String getS3AnalyticsDir(String s3Bucket, String tenantId) {
        return String.format(protocol + s3AnalyticsDir, s3Bucket, tenantId);
    }

    public String getS3AnalyticsModelDir(String s3Bucket, String tenantId) {
        return String.format(protocol + s3EventTableModelDir, s3Bucket, tenantId);
    }

    public String getS3AnalyticsDataDir(String s3Bucket, String tenantId) {
        return String.format(protocol + s3EventTableDataDir, s3Bucket, tenantId);
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
    public String convertAtlasTableDir(String inputTableDir, String pod, String tgtTenantId, String s3Bucket) {
        inputTableDir = getFullPath(inputTableDir);
        StringBuilder builder = new StringBuilder();
        String tenantId = parseTenantIdFromTablePath(inputTableDir);
        if (StringUtils.isBlank(tenantId)) {
            log.warn("Cannot parse tenant id from input table dir " + inputTableDir //
                    + " use target tenant id " + tgtTenantId);
            tenantId = tgtTenantId;
        }
        String hdfsTablesDir = getHdfsAtlasTablesDir(pod, tenantId);
        if (inputTableDir.startsWith(hdfsTablesDir)) {
            return builder.append(getS3AtlasTablesDir(s3Bucket, tenantId))
                    .append(inputTableDir.substring(hdfsTablesDir.length())).toString();
        } else {
            String lastDir = FilenameUtils.getName(inputTableDir);
            return builder.append(getS3AtlasTablesDir(s3Bucket, tenantId)).append(PATH_SEPARATOR).append(lastDir)
                    .toString();
        }
    }

    private String parseTenantIdFromTablePath(String hdfsPath) {
        String tenantId = "";
        if (StringUtils.isNotBlank(hdfsPath)) {
            Pattern pattern = Pattern.compile(".*/Tenants/(?<tenantId>[^/]+)/Spaces/.*");
            Matcher matcher = pattern.matcher(hdfsPath);
            if (matcher.matches()) {
                tenantId = matcher.group("tenantId");
            }
        }
        return tenantId;
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

    public String convertAtlasFileExport(String inputExportFileDir, String pod, String tenantId,
            DropBoxSummary dropBoxSummary, String s3Bucket) {
        StringBuilder builder = new StringBuilder();
        String hdfsExportsDir = getHdfsAtlasFileExportDir(pod, tenantId);
        if (inputExportFileDir.startsWith(hdfsExportsDir)) {
            return builder.append(getS3AtlasFileExportsDir(s3Bucket, dropBoxSummary.getDropBox()))
                    .append(inputExportFileDir.substring(hdfsExportsDir.length())).toString();
        }
        String fileName = FilenameUtils.getName(inputExportFileDir);
        return builder.append(getS3AtlasFileExportsDir(s3Bucket, tenantId)).append(PATH_SEPARATOR).append(fileName)
                .toString();
    }

    public String convertS3CampaignExportDir(String inputExportFile, String s3Bucket, String dropboxName, String playId,
            String playName) {
        return getS3CampaignExportDir(s3Bucket, dropboxName) + PATH_SEPARATOR + playName + FILE_NAME_SEPARATOR + playId
                + FILE_NAME_SEPARATOR + new Date().getTime() + FilenameUtils.EXTENSION_SEPARATOR
                + FilenameUtils.getExtension(inputExportFile);
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

    public String exploreS3FilePath(String filePath, String s3Bucket) {
        try {
            URI uri = new URI(filePath);
            filePath = uri.getPath();
        } catch (Exception ex) {
            log.warn("Bad url=" + filePath);
        }
        filePath = FilenameUtils.normalize(filePath);
        StringBuilder builder = new StringBuilder();

        // try analytics
        Matcher matcher = Pattern.compile("^/user/s-analytics/customers" //
                + "/(?<customerSpace>[^/]+)/(?<tail>.*)").matcher(filePath);
        if (matcher.matches()) {
            String customerSpace = matcher.group("customerSpace");
            String tail = "/" + matcher.group("tail");
            String tenantId = CustomerSpace.parse(customerSpace).getTenantId();
            return builder.append(getS3AnalyticsDir(s3Bucket, tenantId)).append(tail).toString();
        } else {
            log.warn(filePath + " does not match analytics pattern");
        }

        // try atlas
        matcher = Pattern.compile("^/Pods/(?<podId>[^/]+)"//
                + "/Contracts/(?<contractId>[^/]+)" //
                + "/Tenants/(?<tenantId>[^/]+)" //
                + "/Spaces/Production/(?<tail>.*)").matcher(filePath);
        if (matcher.matches()) {
            String tenantId = matcher.group("tenantId");
            String tail = "/" + matcher.group("tail");
            return builder.append(getS3AtlasDir(s3Bucket, tenantId)).append(tail).toString();
        } else {
            log.warn(filePath + " does not match pods pattern");
        }

        return filePath;
    }

    private String getMetadataTableFolderName(String eventTable, String eventColumn) {
        return String.format("%s-%s-Metadata", eventTable.replaceAll("[^A-Za-z0-9_-]", "_"),
                eventColumn.replaceAll("[^A-Za-z0-9_-]", "_"));
    }

    // Some ad hoc methods
    public String getS3PathWithGlob(Configuration yarnConfiguration, String path, boolean isGlob, String s3Bucket) {
        try {
            String s3Path = exploreS3FilePath(path, s3Bucket);
            if (isGlob) {
                if (CollectionUtils.isNotEmpty(HdfsUtils.getFilesByGlob(yarnConfiguration, s3Path))) {
                    path = s3Path;
                }
            } else {
                if (HdfsUtils.fileExists(yarnConfiguration, s3Path)) {
                    path = s3Path;
                }
            }
            return path;
        } catch (Exception ex) {
            log.warn("Could not get S3 path!", ex.getMessage());
        }
        return path;
    }

    public String getS3Dir(Configuration yarnConfiguration, String hdfsPath, String s3Bucket) throws IOException {
        String hdfsDir = getFullPath(hdfsPath);
        String s3Dir = exploreS3FilePath(hdfsDir, s3Bucket);
        String original = hdfsPath;
        if (HdfsUtils.fileExists(yarnConfiguration, s3Dir)) {
            hdfsPath = exploreS3FilePath(hdfsPath, s3Bucket);
            log.info("Use s3 path " + hdfsPath + " instead of the original hdfs path " + original);
        } else {
            log.info("Did not find data at s3 dir " + s3Dir + " fall back to original hfs path " + original);
        }
        return hdfsPath;
    }

    public List<String> toHdfsPaths(List<String> dirs) {
        List<String> newDirs = new ArrayList<>();
        dirs.forEach(dir -> {
            String newDir = toHdfsPath(dir);
            newDirs.add(newDir);
        });
        return newDirs;
    }

    public String toHdfsPath(String dir) {
        if (dir.startsWith(getHdfsAnalyticsBaseDir())) {
            return dir;
        }
        String[] tokens = dir.split("/");
        CustomerSpace space = CustomerSpace.parse(tokens[1]);
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 3; i < tokens.length; i++) {
            strBuilder.append("/").append(tokens[i]);
        }
        return getHdfsAnalyticsDir(space.toString()) + strBuilder.toString();
    }

    public String getCustomerFromHdfsPath(String hdfsPath) {
        String[] tokens = hdfsPath.split("/");
        CustomerSpace space = CustomerSpace.parse(tokens[4]);
        return space.toString();
    }

    public String stripProtocolAndBucket(String s3Path) {
        Matcher matcher = Pattern.compile("^[^:]+://[^/]+/(?<tail>.*)").matcher(s3Path);
        if (matcher.matches()) {
            return matcher.group("tail");
        } else {
            return s3Path;
        }
    }

    public String getProtocol() { return protocol; }

    @VisibleForTesting
    void setProtocol(String protocol) { this.protocol = protocol; }

    public String getPathSeparator() {
        return PATH_SEPARATOR;
    }

    public String getProtocolSeparator() {
        return PROTOCOL_SEPARATOR;
    }
}
