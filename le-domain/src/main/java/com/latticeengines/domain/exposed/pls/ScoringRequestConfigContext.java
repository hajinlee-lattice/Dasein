package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ScoringRequestConfigContext {

    protected String tenantId;
    protected ExternalSystem externalSystem;
    protected String externalProfileId;
    protected String configId;
    protected String modelUuid;
    protected String secretKey;
    
    public ScoringRequestConfigContext() {
        
    }
    
    public ScoringRequestConfigContext(ScoringRequestConfig scoringRequestConfig, MarketoCredential marketoCredential) {
        if (scoringRequestConfig != null) {
            this.configId = scoringRequestConfig.getConfigId();
            this.modelUuid = scoringRequestConfig.getModelUuid();
        }
        if (marketoCredential != null) {
            this.externalSystem = ExternalSystem.Marketo;
            this.externalProfileId = marketoCredential.getPid().toString();
            this.secretKey = marketoCredential.getLatticeSecretKey();
            this.tenantId = marketoCredential.getTenant().getId();
        }
    }
    
    @JsonProperty("requestConfigId")
    public String getConfigId() {
        return configId;
    }

    public void setConfigId(String configId) {
        this.configId = configId;
    }

    @JsonProperty("modelUuid")
    public String getModelUuid() {
        return modelUuid;
    }

    public void setModelUuid(String modelUuid) {
        this.modelUuid = modelUuid;
    }

    @JsonProperty("tenantId")
    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @JsonProperty("externalSystem")
    public ExternalSystem getExternalSystem() {
        return externalSystem;
    }

    public void setExternalSystem(ExternalSystem externalProfile) {
        this.externalSystem = externalProfile;
    }

    @JsonProperty("secretKey")
    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }
    
    public enum ExternalSystem {
        Marketo
    }

    @JsonProperty("externalProfileId")
    public String getExternalProfileId() {
        return externalProfileId;
    }

    public void setExternalProfileId(String externalProfileId) {
        this.externalProfileId = externalProfileId;
    }
}
