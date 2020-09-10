package com.latticeengines.domain.exposed.dante;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.latticeengines.domain.exposed.dante.metadata.MetadataDocument;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize()
public class DanteConfigurationDocument implements Serializable{

    @JsonProperty("metadataDocument")
    private MetadataDocument metadataDocument;

    @JsonProperty("widgetConfigurationDocument")
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

    public DanteConfigurationDocument()
    {
        super();
    }
}
