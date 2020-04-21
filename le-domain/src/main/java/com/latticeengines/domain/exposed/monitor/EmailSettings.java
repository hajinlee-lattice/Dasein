package com.latticeengines.domain.exposed.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class EmailSettings {

    public static final String NEW_USER_SUBJECT = "Lattice Password – New User";
    public static final String EXISTING_USER_SUBJECT = "New Tenant";
    public static final String PLS_NEW_USER_SUBJECT = "Welcome to Lattice Sales and Marketing Platform";
    public static final String PLS_FORGET_PASSWORD_EMAIL_SUBJECT = "Lattice Reset Password – Forgot";
    public static final String PLS_FORGET_PASSWORD_CONFIRMATION_EMAIL_SUBJECT = "Lattice Reset Password – Confirmation";
    public static final String PLS_CREATE_MODEL_EMAIL_JOB_TYPE = "Create Model";
    public static final String PLS_JOB_SUCCESS_EMAIL_SUBJECT = "Job – Success";
    public static final String PLS_JOB_ERROR_EMAIL_SUBJECT = "Job – Error";
    public static final String PLS_SCORE_EMAIL_JOB_TYPE = "Score List";
    public static final String PLS_INTERNAL_ATTRIBUTE_ENRICH_COMPLETION_EMAIL_SUBJECT = "Enrich Internal Attributes – Success";
    public static final String PLS_INTERNAL_ATTRIBUTE_ENRICH_EMAIL_JOB_TYPE = "Enrich Internal Attributes";
    public static final String PLS_INTERNAL_ATTRIBUTE_ENRICH_ERROR_EMAIL_SUBJECT = "Enrich Internal Attributes – Failed";
    public static final String PLS_ONE_TIME_SFDC_ACCESS_TOKEN_EMAIL_SUBJECT = "Lattice one-time authentication token";
    public static final String GLOBAL_AUTH_FORGET_CREDS_EMAIL_SUBJECT = "Lattice Password Reset";
    public static final String PLS_METADATA_SEGMENT_EXPORT_SUCCESS_SUBJECT = "Segment Export – Success";
    public static final String PLS_METADATA_SEGMENT_EXPORT_ERROR_SUBJECT = "Segment Export – Error";
    public static final String PLS_METADATA_SEGMENT_EXPORT_IN_PROGRESS_SUBJECT = "Segment Export – Running";
    public static final String PLS_ALWAYS_ON_CAMPAIGN_EXPIRATION_SUBJECT = "DEACTIVATED - Always On for Campaign - %s ";
    public static final String PLS_METADATA_ORPHAN_RECORDS_EXPORT_SUCCESS_SUBJECT = "Segment Orphan Export – Success";
    public static final String PLS_METADATA_ORPHAN_RECORDS_EXPORT_IN_PROGRESS_SUBJECT = "Segment Orphan Export – Running";
    public static final String CDL_PA_COMPLETION_EMAIL_SUBJECT = "Lattice Job – Success";
    public static final String CDL_PA_ERROR_EMAIL_SUBJECT = "Lattice Job – Error";
    public static final String TENANT_STATE_NOTICE_EMAIL_SUBJECT = "POC Tenant State";
    public static final String S3_CREDENTIALS_EMAIL_SUBJECT = "S3 Credentials – Generated Key";
    public static final String CDL_INGESTION_STATUS_SUBJECT = "Lattice Ingestion Job - %s";
    public static final String S3_TEMPLATE_UPDATE_SUBJECT = "S3 Credentials – Template updated";
    public static final String S3_TEMPLATE_CREATE_SUBJECT = "S3 Credentials – Template created";
    public static final String LATTICE_HELP_CENTER_URL = "http://help.lattice-engines.com";
    public static final String PLS_ACTION_CANCEL_SUCCESS_EMAIL_SUBJECT = "Cancel Action - Success";
    public static final String TENANT_RIGHT_NOTICE_SUBJECT = "Remove tenant for user – Place action";
    public static final String TENANT_RIGHT_REMOVE_SUBJECT = "Remove tenant for user – Removed";
    public static final String DCP_UPLOAD_COMPLETED_SUBJECT = "DCP Upload Completed";
    public static final String DCP_UPLOAD_FAILED_SUBJECT = "DCP Upload Failed";

    private String from;
    private String password;
    private int port;
    private String server;
    private boolean useSTARTTLS;
    private boolean useSSL;
    private String username;

    @JsonProperty("From")
    public String getFrom() {
        return from;
    }

    @JsonProperty("From")
    public void setFrom(String from) {
        this.from = from;
    }

    @JsonProperty("Password")
    public String getPassword() {
        return password;
    }

    @JsonProperty("Password")
    public void setPassword(String password) {
        this.password = password;
    }

    @JsonProperty("Port")
    public int getPort() {
        return port;
    }

    @JsonProperty("Port")
    public void setPort(int port) {
        this.port = port;
    }

    @JsonProperty("Server")
    public String getServer() {
        return server;
    }

    @JsonProperty("Server")
    public void setServer(String server) {
        this.server = server;
    }

    @JsonProperty("UseSSL")
    public boolean isUseSSL() {
        return useSSL;
    }

    @JsonProperty("UseSSL")
    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    @JsonProperty("UseSTARTTLS")
    public boolean isUseSTARTTLS() {
        return useSTARTTLS;
    }

    @JsonProperty("UseSTARTTLS")
    public void setUseSTARTTLS(boolean useSTARTTLS) {
        this.useSTARTTLS = useSTARTTLS;
    }

    @JsonProperty("Username")
    public String getUsername() {
        return username;
    }

    @JsonProperty("Username")
    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
