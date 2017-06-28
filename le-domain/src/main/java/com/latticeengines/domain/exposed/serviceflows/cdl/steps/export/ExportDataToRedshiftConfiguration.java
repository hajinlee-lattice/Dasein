package com.latticeengines.domain.exposed.serviceflows.cdl.steps.export;

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
    private Map<BusinessEntity, Table> entityTableMap;

    @JsonProperty("drop_source_table")
    private Boolean dropSourceTable;

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

    public Boolean getDropSourceTable() {
        return dropSourceTable;
    }

    public void setDropSourceTable(Boolean dropSourceTable) {
        this.dropSourceTable = dropSourceTable;
    }
}
