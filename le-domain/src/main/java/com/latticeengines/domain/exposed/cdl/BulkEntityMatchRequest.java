package com.latticeengines.domain.exposed.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;

/**
 * Request class for performing bulk entity match workflow
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class BulkEntityMatchRequest {

    @JsonProperty("Entity")
    private String entity;

    @JsonProperty("BumpVersion")
    private BumpVersionRequest bumpVersion;

    @JsonProperty("BeforeMatchPublish")
    private EntityPublishRequest beforeMatchPublish;

    @JsonProperty("InputFilePaths")
    private List<String> inputFilePaths;

    @JsonProperty("BulkEntityMatchInput")
    private MatchInput bulkEntityMatchInput;

    @JsonProperty("AfterMatchPublish")
    private EntityPublishRequest afterMatchPublish;

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public BumpVersionRequest getBumpVersion() {
        return bumpVersion;
    }

    public void setBumpVersion(BumpVersionRequest bumpVersion) {
        this.bumpVersion = bumpVersion;
    }

    public EntityPublishRequest getBeforeMatchPublish() {
        return beforeMatchPublish;
    }

    public void setBeforeMatchPublish(EntityPublishRequest beforeMatchPublish) {
        this.beforeMatchPublish = beforeMatchPublish;
    }

    public List<String> getInputFilePaths() {
        return inputFilePaths;
    }

    public void setInputFilePaths(List<String> inputFilePaths) {
        this.inputFilePaths = inputFilePaths;
    }

    public MatchInput getBulkEntityMatchInput() {
        return bulkEntityMatchInput;
    }

    public void setBulkEntityMatchInput(MatchInput bulkEntityMatchInput) {
        this.bulkEntityMatchInput = bulkEntityMatchInput;
    }

    public EntityPublishRequest getAfterMatchPublish() {
        return afterMatchPublish;
    }

    public void setAfterMatchPublish(EntityPublishRequest afterMatchPublish) {
        this.afterMatchPublish = afterMatchPublish;
    }

    public static class BumpVersionRequest {

        @JsonProperty("StagingCustomerSpaces")
        private List<String> stagingCustomerSpaces;

        @JsonProperty("ServingCustomerSpaces")
        private List<String> servingCustomerSpaces;

        public List<String> getStagingCustomerSpaces() {
            return stagingCustomerSpaces;
        }

        public void setStagingCustomerSpaces(List<String> stagingCustomerSpaces) {
            this.stagingCustomerSpaces = stagingCustomerSpaces;
        }

        public List<String> getServingCustomerSpaces() {
            return servingCustomerSpaces;
        }

        public void setServingCustomerSpaces(List<String> servingCustomerSpaces) {
            this.servingCustomerSpaces = servingCustomerSpaces;
        }
    }
}
