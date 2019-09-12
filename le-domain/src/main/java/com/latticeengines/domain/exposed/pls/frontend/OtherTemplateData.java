package com.latticeengines.domain.exposed.pls.frontend;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OtherTemplateData {

    // Internal name of the schema field that columns are mapped to.  Must be unique in each template and unique
    // across all templates representing the same business entity.
    @JsonProperty
    protected String fieldName;

    // The data format of this schema field.
    @JsonProperty
    protected UserDefinedType fieldType;

    // True if this field is already setup as a column in the batch store.
    @JsonProperty
    protected Boolean inBatchStore;

    // The list of existing templates in which this field is already defined.
    @JsonProperty
    protected List<String> existingTemplateNames;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public UserDefinedType getFieldType() {
        return fieldType;
    }

    public void setFieldType(UserDefinedType fieldType) {
        this.fieldType = fieldType;
    }

    public Boolean getInBatchStore() {
        return inBatchStore;
    }

    public void setInBatchStore(Boolean inBatchStore) {
        this.inBatchStore = inBatchStore;
    }

    public List<String> getExistingTemplateNames() {
        return existingTemplateNames;
    }

    public void setExistingTemplateNames(List<String> existingTemplateNames) {
        this.existingTemplateNames = existingTemplateNames;
    }
}
