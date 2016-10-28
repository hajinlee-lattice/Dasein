package com.latticeengines.actors.visitor.sample;

public class SampleDataSourceLookupRequest {
    private Object inputData;
    private SampleMatchTravelerContext matchTravelerContext;
    private String callerMicroEngineReference;

    public Object getInputData() {
        return inputData;
    }

    public void setInputData(Object inputData) {
        this.inputData = inputData;
    }

    public SampleMatchTravelerContext getMatchTravelerContext() {
        return matchTravelerContext;
    }

    public void setMatchTravelerContext(SampleMatchTravelerContext matchTravelerContext) {
        this.matchTravelerContext = matchTravelerContext;
    }

    public String getCallerMicroEngineReference() {
        return callerMicroEngineReference;
    }

    public void setCallerMicroEngineReference(String callerMicroEngineReference) {
        this.callerMicroEngineReference = callerMicroEngineReference;
    }

}
