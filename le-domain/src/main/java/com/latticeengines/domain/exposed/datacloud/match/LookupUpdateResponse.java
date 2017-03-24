package com.latticeengines.domain.exposed.datacloud.match;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LookupUpdateResponse {

    @JsonProperty("Results")
    private List<Result> results;

    public List<Result> getResults() {
        return results;
    }

    public void setResults(List<Result> results) {
        this.results = results;
    }

    public static class Result {
        @JsonProperty("Request")
        private LookupUpdateRequest request;

        @JsonProperty("Success")
        private boolean success;

        @JsonProperty("ErrorMessage")
        private String errorMessage;

        public LookupUpdateRequest getRequest() {
            return request;
        }

        public void setRequest(LookupUpdateRequest request) {
            this.request = request;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }
    }

}
