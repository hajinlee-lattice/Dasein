package com.latticeengines.domain.exposed.admin;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public class TenantRegistration {

    private ContractInfo contractInfo;
    private TenantInfo tenantInfo;
    private CustomerSpaceInfo spaceInfo;
    private SpaceConfiguration spaceConfig;
    private List<SerializableDocumentDirectory> configDirectories;

    @JsonProperty("ContractInfo")
    public ContractInfo getContractInfo() {
        return contractInfo;
    }

    @JsonProperty("ContractInfo")
    public void setContractInfo(ContractInfo contractInfo) {
        this.contractInfo = contractInfo;
    }

    @JsonProperty("TenantInfo")
    public TenantInfo getTenantInfo() {
        return tenantInfo;
    }

    @JsonProperty("TenantInfo")
    public void setTenantInfo(TenantInfo tenantInfo) {
        this.tenantInfo = tenantInfo;
    }

    @JsonProperty("CustomerSpaceInfo")
    public CustomerSpaceInfo getSpaceInfo() {
        return spaceInfo;
    }

    @JsonProperty("CustomerSpaceInfo")
    public void setSpaceInfo(CustomerSpaceInfo spaceInfo) {
        this.spaceInfo = spaceInfo;
    }

    @JsonProperty("SpaceConfig")
    public SpaceConfiguration getSpaceConfig() {
        return spaceConfig;
    }

    @JsonProperty("SpaceConfig")
    public void setSpaceConfig(SpaceConfiguration spaceConfig) {
        this.spaceConfig = spaceConfig;
    }

    @JsonProperty("ConfigDirectories")
    public List<SerializableDocumentDirectory> getConfigDirectories() {
        return configDirectories;
    }

    @JsonProperty("ConfigDirectories")
    public void setConfigDirectories(List<SerializableDocumentDirectory> configDirectories) {
        this.configDirectories = configDirectories;
    }
}
