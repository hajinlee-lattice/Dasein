package com.latticeengines.domain.exposed.camille.lifecycle;

import java.util.List;

import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class TenantInfo {
    public TenantInfo(TenantProperties properties) {
        this.properties = properties;
    }

    // constructor for serialization
    public TenantInfo() {}

    public TenantProperties properties;
    public String contractId;
    public BootstrapState bootstrapState;
    public List<CustomerSpaceInfo> spaceInfoList;
    public ContractInfo contractInfo;
}
