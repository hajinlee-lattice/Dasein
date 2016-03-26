package com.latticeengines.domain.exposed.eai;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;

public class ExportConfiguration extends BasePayloadConfiguration {

    private ExportFormat exportFormat;
    private ExportDestination exportDestination;
    private List<Table> tables;
    private Map<String, String> properties = new HashMap<>();

    @JsonProperty("export_format")
    public ExportFormat getExportFormat() {
        return exportFormat;
    }

    @JsonProperty("export_format")
    public void setExportFormat(ExportFormat exportFormat) {
        this.exportFormat = exportFormat;
    }
    
    @JsonProperty("export_destination")
    public ExportDestination getExportDestination() {
        return exportDestination;
    }

    @JsonProperty("export_destination")
    public void setExportDestination(ExportDestination exportDestination) {
        this.exportDestination = exportDestination;
    }

    @JsonProperty("tables")
    public List<Table> getTables() {
        return tables;
    }

    @JsonProperty("tables")
    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    @JsonProperty("properties")
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonProperty("properties")
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setProperty(String propertyName, String propertyValue) {
        properties.put(propertyName, propertyValue);
    }
}
