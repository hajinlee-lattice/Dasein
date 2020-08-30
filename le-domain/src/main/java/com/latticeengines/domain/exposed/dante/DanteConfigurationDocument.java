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

@JsonDeserialize()
public class DanteConfigurationDocument implements Serializable {

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

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
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
