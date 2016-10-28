package com.latticeengines.actors.exposed.traveler;

public class Response {
    private String requestId;
    private Object result;
    private TravelerContext travelerContext;

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public TravelerContext getTravelerContext() {
        return travelerContext;
    }

    public void setTravelerContext(TravelerContext travelerContext) {
        this.travelerContext = travelerContext;
    }

}
