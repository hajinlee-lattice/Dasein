package com.latticeengines.actors.exposed.traveler;

public class Response {
    private String requestId;
    private Object result;
    private TravelContext travelerContext;

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

    public TravelContext getTravelerContext() {
        return travelerContext;
    }

    public void setTravelerContext(TravelContext travelerContext) {
        this.travelerContext = travelerContext;
    }

}
