package com.latticeengines.domain.exposed.serviceflows.cdl.play;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class PlayLaunchExportFilesToS3Configuration extends ImportExportS3StepConfiguration {

    private String playName;

    private String playDisplayName;

    private String playLaunchId;

    private CDLExternalSystemType playLaunchDestination;

    private LookupIdMap lookupIdMap;

    public String getPlayName() {
        return playName;
    }

    public void setPlayName(String playName) {
        this.playName = playName;
    }

    public String getPlayDisplayName() {
        return playDisplayName;
    }

    public void setPlayDisplayName(String playDisplayName) {
        this.playDisplayName = playDisplayName;
    }

    public String getPlayLaunchId() {
        return playLaunchId;
    }

    public void setPlayLaunchId(String playLaunchId) {
        this.playLaunchId = playLaunchId;
    }

    public LookupIdMap getLookupIdMap() {
        return lookupIdMap;
    }

    public void setLookupIdMap(LookupIdMap lookupIdMap) {
        this.lookupIdMap = lookupIdMap;
    }

    public CDLExternalSystemType getPlayLaunchDestination() {
        return playLaunchDestination;
    }

    public void setPlayLaunchDestination(CDLExternalSystemType playLaunchDestination) {
        this.playLaunchDestination = playLaunchDestination;
    }

}
