package com.latticeengines.domain.exposed.datacloud.manage;

import java.util.List;
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
public class DataBlockEntitlementContainer {

    @JsonProperty("domains")
    private List<Domain> domains;

    public DataBlockEntitlementContainer(List<Domain> domains) {
        this.domains = domains;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect( //
            fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE //
    )
    public static class Domain {

        @JsonProperty("domain")
        private DataDomain domain;

        @JsonProperty("recordTypes")
        private Map<DataRecordType, List<Block>> recordTypes;

        public Domain(DataDomain domain, Map<DataRecordType, List<Block>> recordTypes) {
            this.domain = domain;
            this.recordTypes = recordTypes;
        }

        public DataDomain getDomain() {
            return domain;
        }

        public Map<DataRecordType, List<Block>> getRecordTypes() {
            return recordTypes;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect( //
            fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE //
    )
    public static class Block {

        @JsonProperty("blockId")
        private String blockId;

        @JsonProperty("levels")
        private List<DataBlockLevel> levels;

        public Block(String blockId, List<DataBlockLevel> levels) {
            this.blockId = blockId;
            this.levels = levels;
        }

        public String getBlockId() {
            return blockId;
        }
    }

}
