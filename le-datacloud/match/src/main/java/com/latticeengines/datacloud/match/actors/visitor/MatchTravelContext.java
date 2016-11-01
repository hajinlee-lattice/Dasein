package com.latticeengines.datacloud.match.actors.visitor;

import com.latticeengines.actors.exposed.traveler.TravelContext;

public class MatchTravelContext extends TravelContext {
    private String dataCloudVersion;
    
    private boolean isMatched = false;
    private boolean isProcessed = false;

    public MatchTravelContext(String rootOperationUid) {
        super(rootOperationUid);
    }

    public boolean isMatched() {
        return isMatched;
    }

    public void setMatched(boolean isMatched) {
        this.isMatched = isMatched;
    }


    public boolean isProcessed() {
        return isProcessed;
    }

    public void setProcessed(boolean processed) {
        isProcessed = processed;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }
    
    
}
