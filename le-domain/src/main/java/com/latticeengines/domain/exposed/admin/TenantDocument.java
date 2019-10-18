package com.latticeengines.domain.exposed.admin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public class TenantDocument {

    private ContractInfo contractInfo;
    private TenantInfo tenantInfo;
    private CustomerSpaceInfo spaceInfo;
    private CustomerSpace space;
    private SpaceConfiguration spaceConfig;
    private BootstrapState bootstrapState;

    @JsonProperty("CustomerSpace")
    public CustomerSpace getSpace() {
        return space;
    }

    @JsonProperty("CustomerSpace")
    public void setSpace(CustomerSpace space1) {
        this.space = space1;
    }

    @JsonProperty("SpaceConfiguration")
    public SpaceConfiguration getSpaceConfig() {
        return spaceConfig;
    }

    @JsonProperty("SpaceConfiguration")
    public void setSpaceConfig(SpaceConfiguration spaceConfig) {
        this.spaceConfig = spaceConfig;
    }

    @JsonProperty("ContractInfo")
    public ContractInfo getContractInfo() {
        return contractInfo;
    }

    @JsonProperty("ContractInfo")
    public void setContractInfo(ContractInfo contractInfo1) {
        this.contractInfo = contractInfo1;
    }

    @JsonProperty("TenantInfo")
    public TenantInfo getTenantInfo() {
        return tenantInfo;
    }

    @JsonProperty("TenantInfo")
    public void setTenantInfo(TenantInfo tenantInfo1) {
        this.tenantInfo = tenantInfo1;
    }

    @JsonProperty("CustomerSpaceInfo")
    public CustomerSpaceInfo getSpaceInfo() {
        return spaceInfo;
    }

    @JsonProperty("CustomerSpaceInfo")
    public void setSpaceInfo(CustomerSpaceInfo spaceInfo1) {
        this.spaceInfo = spaceInfo1;
    }

    @JsonProperty("BootstrapState")
    public BootstrapState getBootstrapState() {
        return bootstrapState;
    }

    @JsonProperty("BootstrapState")
    public void setBootstrapState(BootstrapState bootstrapState1) {
        this.bootstrapState = bootstrapState1;
    }

    @JsonIgnore
    public FeatureFlagValueMap getFeatureFlags() {
        CustomerSpaceInfo spaceInfo = getSpaceInfo();
        if (spaceInfo != null) {
            return JsonUtils.deserialize(spaceInfo.featureFlags, FeatureFlagValueMap.class);
        } else {
            return new FeatureFlagValueMap();
        }
    }

    @JsonIgnore
    public void setFeatureFlags(FeatureFlagValueMap featureFlags) {
        CustomerSpaceInfo spaceInfo = getSpaceInfo();
        if (spaceInfo == null) {
            spaceInfo = new CustomerSpaceInfo();
        }
        spaceInfo.featureFlags = JsonUtils.serialize(featureFlags);
    }

}
