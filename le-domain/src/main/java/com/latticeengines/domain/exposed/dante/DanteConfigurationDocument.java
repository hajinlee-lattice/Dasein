package com.latticeengines.domain.exposed.dante;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.latticeengines.domain.exposed.dante.metadata.MetadataDocument;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize()
public class DanteConfigurationDocument {

    private MetadataDocument metadataDocument;
    private String widgetConfigurationDocument;

    @JsonProperty("metadataDocument")
    public MetadataDocument getMetadataDocument() {
        return metadataDocument;
    }

    @JsonProperty("widgetConfigurationDocument")
    public String getWidgetConfigurationDocument() {
        return widgetConfigurationDocument;
    }

    public DanteConfigurationDocument(MetadataDocument metadataDocument, String widgetConfigurationDocument) {
        this.metadataDocument = metadataDocument;
        this.widgetConfigurationDocument = widgetConfigurationDocument;
    }

    public DanteConfigurationDocument()
    {
        super();
    }
}
