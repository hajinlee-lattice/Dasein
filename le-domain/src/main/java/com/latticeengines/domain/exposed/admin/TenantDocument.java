package com.latticeengines.domain.exposed.admin;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public class TenantDocument {

    private ContractInfo contractInfo;
    private TenantInfo tenantInfo;
    private CustomerSpaceInfo spaceInfo;
    private CustomerSpace space;

    @JsonProperty("CustomerSpace")
    public CustomerSpace getSpace() { return space; }

    @JsonProperty("CustomerSpace")
    public void setSpace(CustomerSpace space) {
        this.space = space;
    }

    @JsonProperty("ContractInfo")
    public ContractInfo getContractInfo() { return contractInfo; }

    @JsonProperty("ContractInfo")
    public void setContractInfo(ContractInfo contractInfo) {
        this.contractInfo = contractInfo;
    }

    @JsonProperty("TenantInfo")
    public TenantInfo getTenantInfo() { return tenantInfo; }

    @JsonProperty("TenantInfo")
    public void setTenantInfo(TenantInfo tenantInfo) {
        this.tenantInfo = tenantInfo;
    }

    @JsonProperty("CustomerSpaceInfo")
    public CustomerSpaceInfo getSpaceInfo() { return spaceInfo; }

    @JsonProperty("CustomerSpaceInfo")
    public void setSpaceInfo(CustomerSpaceInfo spaceInfo) { this.spaceInfo = spaceInfo; }
}
