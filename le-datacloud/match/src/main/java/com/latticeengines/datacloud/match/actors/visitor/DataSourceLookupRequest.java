package com.latticeengines.datacloud.match.actors.visitor;

public class DataSourceLookupRequest {
    private Object inputData;
    private MatchTraveler matchTravelerContext;
    private String callerMicroEngineReference;

    public Object getInputData() {
        return inputData;
    }

    public void setInputData(Object inputData) {
        this.inputData = inputData;
    }

    public MatchTraveler getMatchTravelerContext() {
        return matchTravelerContext;
    }

    public void setMatchTravelerContext(MatchTraveler matchTravelerContext) {
        this.matchTravelerContext = matchTravelerContext;
    }

    public String getCallerMicroEngineReference() {
        return callerMicroEngineReference;
    }

    public void setCallerMicroEngineReference(String callerMicroEngineReference) {
        this.callerMicroEngineReference = callerMicroEngineReference;
    }

}
