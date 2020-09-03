package com.latticeengines.domain.exposed.dante;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dante.metadata.MetadataDocument;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.IsColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

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
