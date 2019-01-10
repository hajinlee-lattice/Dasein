package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;

public class PlayLaunchExportFilesGeneratorConfiguration extends PlayLaunchInitStepConfiguration {

    private String destinationOrgId;
    private CDLExternalSystemType destinationSysType;

    public String getDestinationOrgId() {
        return destinationOrgId;
    }

    public void setDestinationOrgId(String destinationOrgId) {
        this.destinationOrgId = destinationOrgId;
    }

    public CDLExternalSystemType getDestinationSysType() {
        return destinationSysType;
    }

    public void setDestinationSysType(CDLExternalSystemType destinationSysType) {
        this.destinationSysType = destinationSysType;
    }

}
