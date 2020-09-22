package com.latticeengines.domain.exposed.dante;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dante.metadata.MetadataDocument;

@JsonIgnoreProperties(ignoreUnknown = true)

public class DanteConfigurationDocument {

    @JsonProperty("MetadataDocument")
    private MetadataDocument metadataDocument;

    @JsonProperty("WidgetConfigurationDocument")
    private String widgetConfigurationDocument;

    public MetadataDocument getMetadataDocument() {
        return metadataDocument;
    }

    public String getWidgetConfigurationDocument() {
        return widgetConfigurationDocument;
    }

    public DanteConfigurationDocument(MetadataDocument metadataDocument, String widgetConfigurationDocument) {
        this.metadataDocument = metadataDocument;
        this.widgetConfigurationDocument = widgetConfigurationDocument;
    }

    public DanteConfigurationDocument() {
        super();
    }
}
