package com.latticeengines.domain.exposed.serviceflows.cdl.steps.export;

import java.util.Collections;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ExportDataToRedshiftConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("redshift_table_config")
    private HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration;

    @JsonProperty("source_tables_map")
    private Map<BusinessEntity, Table> entityTableMap = Collections.emptyMap();

    @JsonProperty("target_table_name")
    private String targetTableName;

    @JsonProperty("append_flag_map")
    private Map<BusinessEntity, Boolean> appendFlagMap = Collections.emptyMap();

    public HdfsToRedshiftConfiguration getHdfsToRedshiftConfiguration() {
        return hdfsToRedshiftConfiguration;
    }

    public void setHdfsToRedshiftConfiguration(HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration) {
        this.hdfsToRedshiftConfiguration = hdfsToRedshiftConfiguration;
    }

    public Map<BusinessEntity, Table> getSourceTables() {
        return entityTableMap;
    }

    public void setSourceTables(Map<BusinessEntity, Table> entityTableMap) {
        this.entityTableMap = entityTableMap;
    }

    public Map<BusinessEntity, Boolean> getAppendFlagMap() {
        return appendFlagMap;
    }

    public void setAppendFlagMap(Map<BusinessEntity, Boolean> appendFlagMap) {
        this.appendFlagMap = appendFlagMap;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }
}
