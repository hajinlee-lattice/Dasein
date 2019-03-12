package com.latticeengines.domain.exposed.metadata.datastore;

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
public class HdfsDataUnit extends DataUnit {

    @JsonProperty("Path")
    private String path;

    public static HdfsDataUnit fromPath(String path) {
        HdfsDataUnit dataUnit = new HdfsDataUnit();
        dataUnit.setPath(path);
        return dataUnit;
    }

    @Override
    @JsonProperty("StorageType")
    public StorageType getStorageType() {
        return StorageType.Hdfs;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

}
