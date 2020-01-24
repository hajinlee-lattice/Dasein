package com.latticeengines.domain.exposed.eai;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.JsonUtils;

public final class ImportConfigurationFactory {

    protected ImportConfigurationFactory() {
        throw new UnsupportedOperationException();
    }

    public static ImportConfiguration getImportConfiguration(SourceType sourceType,
            String configStr) {
        switch (sourceType) {
            case VISIDB:
                return getVdbImportConfiguration();
            case FILE:
                return getCSVImportConfiguration(configStr);
            default:
                throw new RuntimeException(
                        String.format("Source %s not supported!", sourceType.getName()));
        }
    }

    private static VdbToHdfsConfiguration getVdbImportConfiguration() {
        VdbToHdfsConfiguration vdbToHdfsConfiguration = new VdbToHdfsConfiguration();
        SourceImportConfiguration sourceImportConfig = new SourceImportConfiguration();
        sourceImportConfig.setSourceType(SourceType.VISIDB);
        vdbToHdfsConfiguration.addSourceConfiguration(sourceImportConfig);
        return vdbToHdfsConfiguration;
    }

    private static CSVToHdfsConfiguration getCSVImportConfiguration(String configStr) {
        CSVToHdfsConfiguration csvToHdfsConfiguration = JsonUtils.deserialize(configStr,
                CSVToHdfsConfiguration.class);
        SourceImportConfiguration sourceImportConfig = new SourceImportConfiguration();
        sourceImportConfig.setSourceType(SourceType.FILE);
        if (StringUtils.isNotEmpty(csvToHdfsConfiguration.getFileSource())
                && csvToHdfsConfiguration.getFileSource().equalsIgnoreCase("S3")) {
            S3FileToHdfsConfiguration s3FileToHdfsConfiguration = (S3FileToHdfsConfiguration) csvToHdfsConfiguration;
            s3FileToHdfsConfiguration.addSourceConfiguration(sourceImportConfig);
            return s3FileToHdfsConfiguration;
        }
        csvToHdfsConfiguration.addSourceConfiguration(sourceImportConfig);
        return csvToHdfsConfiguration;
    }
}
