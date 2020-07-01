package com.latticeengines.domain.exposed.datacloud.manage;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;


// this is a front-end object
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class DataBlockMetadataContainer {

    @JsonProperty("blocks")
    private Map<String, DataBlock> blocks;

    public Map<String, DataBlock> getBlocks() {
        return blocks;
    }

    public void setBlocks(Map<String, DataBlock> blocks) {
        this.blocks = blocks;
    }
}
