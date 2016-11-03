package com.latticeengines.actors.visitor.sample;

import com.latticeengines.actors.exposed.traveler.Traveler;

public class SampleMatchTravelContext extends Traveler {
    private SampleMatchKeyTuple matchKeyTuple;
    private String dataCloudVersion;

    private boolean isMatched = false;
    private boolean isProcessed = false;

    public SampleMatchTravelContext(String rootOperationUid) {
        super(rootOperationUid);
    }

    @Override
    protected Object getInputData() {
        return matchKeyTuple;
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

    public SampleMatchKeyTuple getMatchKeyTuple() {
        return matchKeyTuple;
    }

    public void setMatchKeyTuple(SampleMatchKeyTuple matchKeyTuple) {
        this.matchKeyTuple = matchKeyTuple;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }
}
