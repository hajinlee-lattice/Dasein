package com.latticeengines.domain.exposed.dcp.vbo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonInclude
public class VboCallback {

    @JsonProperty("customer_creation")
    public CustomerCreation customerCreation;

    @JsonIgnore
    public Boolean timeout;

    @JsonIgnore
    public String targetUrl;

    public static class CustomerCreation {
        @JsonProperty("transaction_detail")
        public TransactionDetail transactionDetail;

        @JsonProperty("customer_detail")
        public CustomerDetail customerDetail;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TransactionDetail {
        @JsonProperty("ackReferenceId")
        public String ackRefId;

        @JsonProperty("status")
        public VboStatus status;

        // Because VBO wants both "ackReferenceId" and "AckReferenceId"
        @JsonProperty("AckReferenceId")
        public String getAckRefId() {
            return ackRefId;
        }
    }

    public static class CustomerDetail {
        @JsonProperty("subscriber_number")
        public String subscriberNumber;

        @JsonProperty("workspace_country")
        public String workspaceCountry;

        @JsonProperty("first_name")
        public String firstName;

        @JsonProperty("middle_name")
        public String middleName;

        @JsonProperty("last_name")
        public String lastName;

        @JsonProperty("login")
        public String login;

        @JsonProperty("welcome_email_sent")
        public String emailSent;

        @JsonProperty("welcome_email_date")
        public String emailDate;
    }

    public String toString() {
        return JsonUtils.serialize(this);
    }
}
