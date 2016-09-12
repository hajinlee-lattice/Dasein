package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;

public class CascadingBulkMatchDataflowParameters extends DataFlowParameters {

    @SourceTableName
    @NotEmptyString
    @NotNull
    private String inputAvro;

    @SourceTableName
    @NotEmptyString
    @NotNull
    public String accountMasterLookup;

    @SourceTableName
    @NotEmptyString
    @NotNull
    private String accountMaster;

    @SourceTableName
    public String publicDomainPath;

    @NotNull
    private String outputSchemaPath;

    @NotNull
    private Boolean returnUnmatched;

    @NotNull
    private Boolean excludePublicDomains;

    private String yarnQueue;

    private Map<MatchKey, List<String>> keyMap;

    @JsonProperty("input_avro")
    public String getInputAvro() {
        return inputAvro;
    }

    @JsonProperty("input_avro")
    public void setInputAvro(String inputAvro) {
        this.inputAvro = inputAvro;
    }

    @JsonProperty("account_master_lookup")
    public String getAccountMasterLookup() {
        return accountMasterLookup;
    }

    @JsonProperty("account_master_lookup")
    public void setAccountMasterLookup(String accountMasterLookup) {
        this.accountMasterLookup = accountMasterLookup;
    }

    @JsonProperty("account_master")
    public String getAccountMaster() {
        return accountMaster;
    }

    @JsonProperty("account_master")
    public void setAccountMaster(String accountMaster) {
        this.accountMaster = accountMaster;
    }

    @JsonProperty("yarn_queue")
    public String getYarnQueue() {
        return yarnQueue;
    }

    @JsonProperty("yarn_queue")
    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }

    @JsonProperty("return_unmatched")
    public Boolean getReturnUnmatched() {
        return returnUnmatched;
    }

    @JsonProperty("return_unmatched")
    public void setReturnUnmatched(Boolean returnUnmatched) {
        this.returnUnmatched = returnUnmatched;
    }

    @JsonProperty("exclude_public_domains")
    public Boolean getExcludePublicDomains() {
        return excludePublicDomains;
    }

    @JsonProperty("exclude_public_domains")
    public void setExcludePublicDomains(Boolean excludePublicDomains) {
        this.excludePublicDomains = excludePublicDomains;
    }

    @JsonProperty("output_schema")
    public void setOutputSchemaPath(String outputSchemaPath) {
        this.outputSchemaPath = outputSchemaPath;
    }

    @JsonProperty("output_schema")
    public String getOutputSchemaPath() {
        return this.outputSchemaPath;
    }

    @JsonProperty("public_domain")
    public void setPublicDomainPath(String publicDomainPath) {
        this.publicDomainPath = publicDomainPath;
    }

    @JsonProperty("public_domain")
    public String getPublicDomainPath() {
        return this.publicDomainPath;
    }

    @JsonProperty("key_map")
    public void setKeyMap(Map<MatchKey, List<String>> keyMap) {
        this.keyMap = keyMap;
    }

    @JsonProperty("key_map")
    public Map<MatchKey, List<String>> getKeyMap() {
        return keyMap;
    }

}
