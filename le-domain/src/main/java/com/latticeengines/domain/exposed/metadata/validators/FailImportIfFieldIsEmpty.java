package com.latticeengines.domain.exposed.metadata.validators;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.latticeengines.domain.exposed.metadata.Table;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class FailImportIfFieldIsEmpty extends InputValidator {

    public FailImportIfFieldIsEmpty() {

    }

    @Override
    public boolean validate(String field, Map<String, String> row, Table metadata) {
        Object value = row.get(metadata.getAttribute(field).getDisplayName());
        return (value != null) && !value.toString().equals("");
    }
}
