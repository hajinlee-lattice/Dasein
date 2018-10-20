package com.latticeengines.domain.exposed.serviceapps.lp;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ModelFieldsToAttributesRequest {

    @JsonProperty("Fields")
    private List<VdbMetadataField> fields;

    @JsonProperty("Attributes")
    private List<Attribute> attributes;

    public List<VdbMetadataField> getFields() {
        return fields;
    }

    public void setFields(List<VdbMetadataField> fields) {
        this.fields = fields;
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<Attribute> attributes) {
        this.attributes = attributes;
    }
}
