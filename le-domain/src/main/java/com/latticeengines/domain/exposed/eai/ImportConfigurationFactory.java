package com.latticeengines.domain.exposed.eai;

import com.latticeengines.common.exposed.util.JsonUtils;

public final class ImportConfigurationFactory {

    public static ImportConfiguration getImportConfiguration(SourceType sourceType, String configStr) {
        switch (sourceType) {
            case VISIDB:
                return getVdbImportConfiguration();
            case FILE:
                return getCSVImportConfiguration(configStr);
            default:
                throw new RuntimeException(String.format("Source %s not supported!", sourceType.getName()));
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
        csvToHdfsConfiguration.addSourceConfiguration(sourceImportConfig);
        return csvToHdfsConfiguration;
    }
}
