package com.latticeengines.domain.exposed.remote.tray;

public class TraySettings {

    private String userToken;

    private String solutionInstanceId;

    private String authenticationId;

    private String clientMutationId;

    public TraySettings(String userToken, String solutionInstanceId, String authenticationId, String clientMutationId) {
        this.userToken = userToken;
        this.solutionInstanceId = solutionInstanceId;
        this.authenticationId = authenticationId;
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

    public void setAuthenticationId(String authenticationId) {
        this.authenticationId = authenticationId;
    }

    public String getAuthenticationId() {
        return this.authenticationId;
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

        private String authenticationId;

        private String clientMutationId;

        public TraySettingsBuilder userToken(String userToken) {
            this.userToken = userToken;
            return this;
        }

        public TraySettingsBuilder solutionInstanceId(String solutionInstanceId) {
            this.solutionInstanceId = solutionInstanceId;
            return this;
        }

        public TraySettingsBuilder authenticationId(String authenticationId) {
            this.authenticationId = authenticationId;
            return this;
        }

        public TraySettingsBuilder clientMutationId(String clientMutationId) {
            this.clientMutationId = clientMutationId;
            return this;
        }

        public TraySettings build() {
            return new TraySettings(this.userToken, this.solutionInstanceId, this.authenticationId,
                    this.clientMutationId);
        }

    }

}
