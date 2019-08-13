package com.latticeengines.domain.exposed.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class EmailSettings {

    public static final String PLS_NEW_INTERNAL_USER_EMAIL_MSG = "You have been added to the <strong>%s</strong> Tenant.";
    public static final String PLS_NEW_USER_SUBJECT = "Welcome to Lattice Sales and Marketing Platform";
    public static final String PLS_NEW_EXTERNAL_USER_EMAIL_MSG = "Congratulations! You've been invited to use Lattice Engines. You can sign in for the first time using the temporary credentials listed below.\n";
    public static final String PLS_NEW_PROSPECTING_USER_EMAIL_MSG = "Congratulations! You've been invited to access Lattice Help Center. You can sign in for the first time using the temporary credentials listed below.\n";
    public static final String PLS_EXISTING_USER_SUBJECT = "Invitation to Access %s (Lattice Sales and Marketing Platform)";
    public static final String PLS_FORGET_PASSWORD_EMAIL_SUBJECT = "Password Reset for Lattice Sales and Marketing Platform";
    public static final String PD_NEW_USER_EMAIL_MSG = "You have been granted access to the Lattice Prospect Discovery.";
    public static final String PD_NEW_USER_SUBJECT = "Welcome to Lattice Prospect Discovery";
    public static final String PLS_IMPORT_DATA_SUCCESS_EMAIL_MSG = "Your Salesforce Data Import is complete.";
    public static final String PLS_IMPORT_DATA_SUCCESS_CURRENT_STEP = "The system is currently enriching the data with Lattice Data Cloud.";
    public static final String PLS_IMPORT_DATA_SUCCESS_EMAIL_SUBJECT = "Lead Prioritization - Import Data Successfully";
    public static final String PLS_IMPORT_DATA_ERROR_EMAIL_SUBJECT = "Lead Prioritization - Import Data Failed";
    public static final String PLS_IMPORT_DATA_ERROR_EMAIL_MSG = "Unable to import data.";
    public static final String PLS_ERROR_EMAIL_LINK_MSG = "Sign in to Lattice to retry.";
    public static final String PLS_ERROR_EMAIL_LINK_JOB = "tenant deployment";
    public static final String PLS_ENRICH_DATA_SUCCESS_EMAIL_SUBJECT = "Lead Prioritization - Enrich Data Successfully";
    public static final String PLS_ENRICH_DATA_SUCCESS_EMAIL_MSG = "Your Data Enrichment is complete.";
    public static final String PLS_ENRICH_DATA_SUCCESS_EMAIL_CURRENT_STEP = "The system is currently validating metadata.";
    public static final String PLS_ENRICH_DATA_ERROR_EMAIL_SUBJECT = "Lead Prioritization - Enrich Data Failed";
    public static final String PLS_ENRICH_DATA_ERROR_EMAIL_MSG = "Unable to enrich data.";
    public static final String PLS_VALIDATE_METADATA_SUCCESS_EMAIL_MSG = "Your Metadata Validation is complete.";
    public static final String PLS_VALIDATE_METADATA_SUCCESS_EMAIL_SUBJECT = "Lead Prioritization - Metadata Validation Successfully";
    public static final String PLS_METADATA_MISSING_EMAIL_SUBJECT = "Lead Prioritization - Metadata Missing";
    public static final String PLS_METADATA_MISSING_EMAIL_MSG = "Missing data.";
    public static final String PLS_METADATA_MISSING_EMAIL_LINK_MSG = "Sign in to Lattice to add missing data.";
    public static final String PLS_VALIDATE_METADATA_ERROR_EMAIL_MSG = "Unable to validate metadata.";
    public static final String PLS_VALIDATE_METADATA_ERROR_EMAIL_SUBJECT = "Lead Prioritization - Validate Metadata Failed";
    public static final String PLS_CREATE_MODEL_EMAIL_JOB_TYPE = "Create Model";
    public static final String PLS_CREATE_MODEL_COMPLETION_EMAIL_MSG = "We have completed the model creation.";
    public static final String PLS_CREATE_MODEL_COMPLETION_EMAIL_SUBJECT = "SUCCESS - Create Model - %s ";
    public static final String PLS_CREATE_MODEL_ERROR_EMAIL_MSG = "Failed to create a model.";
    public static final String PLS_CREATE_MODEL_ERROR_EMAIL_SUBJECT = "FAILURE - Create Model - %s ";
    public static final String PLS_SCORE_COMPLETION_EMAIL_SUBJECT = "SUCCESS - Score List - %s ";
    public static final String PLS_SCORE_EMAIL_JOB_TYPE = "Score List";
    public static final String PLS_SCORE_COMPLETION_EMAIL_MSG = "We have completed the scoring.";
    public static final String PLS_SCORE_ERROR_EMAIL_SUBJECT = "FAILURE - Score List - %s ";
    public static final String PLS_SCORE_ERROR_EMAIL_MSG = "Failed to score.";
    public static final String PLS_INTERNAL_ATTRIBUTE_ENRICH_COMPLETION_EMAIL_SUBJECT = "SUCCESS - Enrich Internal Attributes - %s ";
    public static final String PLS_INTERNAL_ATTRIBUTE_ENRICH_EMAIL_JOB_TYPE = "Enrich Internal Attributes";
    public static final String PLS_INTERNAL_ATTRIBUTE_ENRICH_COMPLETION_EMAIL_MSG = "We have completed the enrichment of internal attributes.";
    public static final String PLS_INTERNAL_ATTRIBUTE_ENRICH_ERROR_EMAIL_SUBJECT = "FAILURE - Enrich Internal Attributes - %s ";
    public static final String PLS_INTERNAL_ATTRIBUTE_ENRICH_ERROR_EMAIL_MSG = "Failed to enrich internal attributes.";
    public static final String PLS_ONE_TIME_SFDC_ACCESS_TOKEN_EMAIL_SUBJECT = "Lattice Application Authentication Token";
    public static final String GLOBAL_AUTH_FORGET_CREDS_EMAIL_SUBJECT = "Lattice Password Reset";
    public static final String PLS_METADATA_SEGMENT_EXPORT_SUCCESS_SUBJECT = "SUCCESS - Segment Export - %s ";
    public static final String PLS_METADATA_SEGMENT_EXPORT_ERROR_SUBJECT = "FAILURE - Segment Export - %s ";
    public static final String PLS_METADATA_SEGMENT_EXPORT_IN_PROGRESS_SUBJECT = "IN PROGRESS - Segment Export - %s ";
    public static final String PLS_METADATA_ORPHAN_RECORDS_EXPORT_SUCCESS_SUBJECT = "SUCCESS - %s Export";
    public static final String PLS_METADATA_ORPHAN_RECORDS_EXPORT_ERROR_SUBJECT = "FAILURE - %s Export";
    public static final String PLS_METADATA_ORPHAN_RECORDS_EXPORT_IN_PROGRESS_SUBJECT = "IN PROGRESS - %s Export";
    public static final String CDL_PA_COMPLETION_EMAIL_SUBJECT = "SUCCESS - Lattice Platform Data Refresh";
    public static final String CDL_PA_ERROR_EMAIL_SUBJECT = "FAILURE - Lattice Platform Data Refresh";
    public static final String TENANT_STATE_NOTICE_EMAIL_SUBJECT = "Tenant State Change";
    public static final String S3_CREDENTIALS_EMAIL_SUBJECT = "File Transfer Credentials";
    public static final String CDL_INGESTION_STATUS_SUBJECT = " %s - %s File Transfer";
    public static final String S3_TEMPLATE_UPDATE_SUBJECT = " Template %s has been updated";
    public static final String S3_TEMPLATE_CREATE_SUBJECT = " Template %s has been created";
    public static final String LATTICE_HELP_CENTER_URL = "http://help.lattice-engines.com";
    public static final String PLS_ACTION_CANCEL_SUCCESS_EMAIL_SUBJECT = "Import Action Cancelled on %s ";
    public static final String TENANT_RIGHT_NOTICE_SUBJECT = "TENANT IN USER STATUS";

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
