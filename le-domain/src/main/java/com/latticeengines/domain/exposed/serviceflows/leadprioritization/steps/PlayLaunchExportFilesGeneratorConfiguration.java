package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;

public class PlayLaunchExportFilesGeneratorConfiguration extends PlayLaunchInitStepConfiguration {

    private String destinationOrgId;
    private CDLExternalSystemType destinationSysType;
    private CDLExternalSystemName destinationSysName;

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

    public CDLExternalSystemName getDestinationSysName() {
        return destinationSysName;
    }

    public void setDestinationSysName(CDLExternalSystemName destinationSysName) {
        this.destinationSysName = destinationSysName;
    }

}
