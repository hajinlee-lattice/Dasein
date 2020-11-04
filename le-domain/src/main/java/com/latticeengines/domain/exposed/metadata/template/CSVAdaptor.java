package com.latticeengines.domain.exposed.metadata.template;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class CSVAdaptor implements Serializable {

    private static final long serialVersionUID = 4689704177336776595L;

    @JsonProperty("importFieldMappings")
    private List<ImportFieldMapping> importFieldMappings;


    public List<ImportFieldMapping> getImportFieldMappings() {
        return importFieldMappings;
    }

    public void setImportFieldMappings(List<ImportFieldMapping> importFieldMappings) {
        this.importFieldMappings = importFieldMappings;
    }
}
