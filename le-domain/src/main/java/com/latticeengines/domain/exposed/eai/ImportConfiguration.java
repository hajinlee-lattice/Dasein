package com.latticeengines.domain.exposed.eai;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ImportConfiguration extends BasePayloadConfiguration {

    private List<SourceImportConfiguration> sourceConfigurations = new ArrayList<>();
    private Map<String, String> properties = new HashMap<>();
    private ImportType importType = ImportType.ImportTable;
    private CamelRouteConfiguration camelRouteConfiguration;

    @JsonProperty("sources")
    public List<SourceImportConfiguration> getSourceConfigurations() {
        return sourceConfigurations;
    }

    @JsonProperty("sources")
    public void setSourceConfigurations(List<SourceImportConfiguration> sourceConfigurations) {
        this.sourceConfigurations = sourceConfigurations;
    }

    @JsonIgnore
    public void addSourceConfiguration(SourceImportConfiguration sourceConfiguration) {
        sourceConfigurations.add(sourceConfiguration);
    }

    @JsonProperty("properties")
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonProperty("properties")
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @JsonIgnore
    public void setProperty(String key, String value) {
        properties.put(key, value);
    }

    @JsonIgnore
    public String getProperty(String key) {
        return properties.get(key);
    }

    @JsonProperty("import_type")
    public ImportType getImportType() {
        return importType;
    }

    @JsonProperty("import_type")
    public void setImportType(ImportType importType) {
        this.importType = importType;
    }

    @JsonProperty("camel_route_configuration")
    public CamelRouteConfiguration getCamelRouteConfiguration() {
        return camelRouteConfiguration;
    }

    @JsonProperty("camel_route_configuration")
    public void setCamelRouteConfiguration(CamelRouteConfiguration camelRouteConfiguration) {
        this.camelRouteConfiguration = camelRouteConfiguration;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public static <T extends CamelRouteConfiguration> ImportConfiguration createForCamelRouteConfiguration(
            T camelRouteConfiguration) {
        ImportConfiguration importConfiguration = new ImportConfiguration();
        importConfiguration.setImportType(ImportType.CamelRoute);
        importConfiguration.setCamelRouteConfiguration(camelRouteConfiguration);
        return importConfiguration;
    }

    public enum ImportType {
        ImportTable, CamelRoute
    }

}
