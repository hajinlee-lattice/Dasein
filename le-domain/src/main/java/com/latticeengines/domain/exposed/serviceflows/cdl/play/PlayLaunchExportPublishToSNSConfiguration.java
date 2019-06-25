package com.latticeengines.domain.exposed.serviceflows.cdl.play;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class PlayLaunchExportPublishToSNSConfiguration extends BaseStepConfiguration {

    private CustomerSpace customerSpace;

    private LookupIdMap lookupIdMap;

    private String externalAudienceId;

    private String externalAudienceName;

    private String externalFolderName;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public LookupIdMap getLookupIdMap() {
        return lookupIdMap;
    }

    public void setLookupIdMap(LookupIdMap lookupIdMap) {
        this.lookupIdMap = lookupIdMap;
    }

    public String getExternalAudienceId() {
        return externalAudienceId;
    }

    public void setExternalAudienceId(String externalAudienceId) {
        this.externalAudienceId = externalAudienceId;
    }

    public String getExternalAudienceName() {
        return externalAudienceName;
    }

    public void setExternalAudienceName(String externalAudienceName) {
        this.externalAudienceName = externalAudienceName;
    }

    public String getExternalFolderName() {
        return externalFolderName;
    }

    public void setExternalFolderName(String externalFolderName) {
        this.externalFolderName = externalFolderName;
    }

}
