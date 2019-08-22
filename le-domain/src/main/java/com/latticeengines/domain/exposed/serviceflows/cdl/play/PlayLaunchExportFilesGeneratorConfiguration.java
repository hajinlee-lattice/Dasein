package com.latticeengines.domain.exposed.serviceflows.cdl.play;

import java.util.Map;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;

public class PlayLaunchExportFilesGeneratorConfiguration extends PlayLaunchInitStepConfiguration {

    private String destinationOrgId;
    private CDLExternalSystemType destinationSysType;
    private CDLExternalSystemName destinationSysName;
    private Map<String, String> accountDisplayNames;
    private Map<String, String> contactDisplayNames;
    private ChannelConfig channelConfig;

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

    public Map<String, String> getAccountDisplayNames() {
        return accountDisplayNames;
    }

    public void setAccountDisplayNames(Map<String, String> accountDisplayNames) {
        this.accountDisplayNames = accountDisplayNames;
    }

    public Map<String, String> getContactDisplayNames() {
        return contactDisplayNames;
    }

    public void setContactDisplayNames(Map<String, String> contactDisplayNames) {
        this.contactDisplayNames = contactDisplayNames;
    }

    public ChannelConfig getChannelConfig() {
        return channelConfig;
    }

    public void setChannelConfig(ChannelConfig channelConfig) {
        this.channelConfig = channelConfig;
    }
}
