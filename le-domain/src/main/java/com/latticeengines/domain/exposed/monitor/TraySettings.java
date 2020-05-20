package com.latticeengines.domain.exposed.monitor;

public class TraySettings {

    private String userToken;

    private String solutionInstanceId;

    private String clientMutationId;

    public TraySettings(String userToken, String solutionInstanceId, String clientMutationId) {
        this.userToken = userToken;
        this.solutionInstanceId = solutionInstanceId;
        this.clientMutationId = clientMutationId;
    }

    public void setUserToken(String userToken) {
        this.userToken = userToken;
    }

    public String getUserToken() {
        return this.userToken;
    }

    public void setSolutionInstanceId(String solutionInstanceId) {
        this.solutionInstanceId = solutionInstanceId;
    }

    public String getSolutionInstanceId() {
        return this.solutionInstanceId;
    }

    public void setClientMutationId(String clientMutationId) {
        this.clientMutationId = clientMutationId;
    }

    public String getClientMutationId() {
        return this.clientMutationId;
    }

    public static class TraySettingsBuilder {

        private String userToken;

        private String solutionInstanceId;

        private String clientMutationId;

        public TraySettingsBuilder userToken(String userToken) {
            this.userToken = userToken;
            return this;
        }

        public TraySettingsBuilder solutionInstanceId(String solutionInstanceId) {
            this.solutionInstanceId = solutionInstanceId;
            return this;
        }

        public TraySettingsBuilder clientMutationId(String clientMutationId) {
            this.clientMutationId = clientMutationId;
            return this;
        }

        public TraySettings build() {
            return new TraySettings(this.userToken, this.solutionInstanceId, this.clientMutationId);
        }

    }

}
