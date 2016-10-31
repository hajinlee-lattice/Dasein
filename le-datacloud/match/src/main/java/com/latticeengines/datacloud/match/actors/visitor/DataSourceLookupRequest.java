package com.latticeengines.datacloud.match.actors.visitor;

public class DataSourceLookupRequest {
    private Object inputData;
    private MatchTravelContext matchTravelerContext;
    private String callerMicroEngineReference;

    public Object getInputData() {
        return inputData;
    }

    public void setInputData(Object inputData) {
        this.inputData = inputData;
    }

    public MatchTravelContext getMatchTravelerContext() {
        return matchTravelerContext;
    }

    public void setMatchTravelerContext(MatchTravelContext matchTravelerContext) {
        this.matchTravelerContext = matchTravelerContext;
    }

    public String getCallerMicroEngineReference() {
        return callerMicroEngineReference;
    }

    public void setCallerMicroEngineReference(String callerMicroEngineReference) {
        this.callerMicroEngineReference = callerMicroEngineReference;
    }

}
