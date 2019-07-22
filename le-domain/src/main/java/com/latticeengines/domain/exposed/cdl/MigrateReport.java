package com.latticeengines.domain.exposed.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class MigrateReport {

    @JsonProperty("input_account_templates")
    private List<String> inputAccountTemplates;

    @JsonProperty("output_account_template")
    private String outputAccountTemplate;

    @JsonProperty("account_counts")
    private Long accountCounts;

    @JsonProperty("account_action_id")
    private Long accountActionId;

    @JsonProperty("input_contact_templates")
    private List<String> inputContactTemplates;

    @JsonProperty("output_contact_template")
    private String outputContactTemplate;

    @JsonProperty("contact_counts")
    private Long contactCounts;

    @JsonProperty("contact_action_id")
    private Long contactActionId;


    @JsonProperty("input_transaction_templates")
    private List<String> inputTransactionTemplates;

    @JsonProperty("output_transaction_template")
    private String outputTransactionTemplate;

    @JsonProperty("transaction_counts")
    private Long transactionCounts;

    @JsonProperty("transaction_action_id")
    private Long transactionActionId;

    public List<String> getInputAccountTemplates() {
        return inputAccountTemplates;
    }

    public void setInputAccountTemplates(List<String> inputAccountTemplates) {
        this.inputAccountTemplates = inputAccountTemplates;
    }

    public String getOutputAccountTemplate() {
        return outputAccountTemplate;
    }

    public void setOutputAccountTemplate(String outputAccountTemplate) {
        this.outputAccountTemplate = outputAccountTemplate;
    }

    public Long getAccountCounts() {
        return accountCounts;
    }

    public void setAccountCounts(Long accountCounts) {
        this.accountCounts = accountCounts;
    }

    public Long getAccountActionId() {
        return accountActionId;
    }

    public void setAccountActionId(Long accountActionId) {
        this.accountActionId = accountActionId;
    }

    public List<String> getInputContactTemplates() {
        return inputContactTemplates;
    }

    public void setInputContactTemplates(List<String> inputContactTemplates) {
        this.inputContactTemplates = inputContactTemplates;
    }

    public String getOutputContactTemplate() {
        return outputContactTemplate;
    }

    public void setOutputContactTemplate(String outputContactTemplate) {
        this.outputContactTemplate = outputContactTemplate;
    }

    public Long getContactCounts() {
        return contactCounts;
    }

    public void setContactCounts(Long contactCounts) {
        this.contactCounts = contactCounts;
    }

    public Long getContactActionId() {
        return contactActionId;
    }

    public void setContactActionId(Long contactActionId) {
        this.contactActionId = contactActionId;
    }

    public List<String> getInputTransactionTemplates() {
        return inputTransactionTemplates;
    }

    public void setInputTransactionTemplates(List<String> inputTransactionTemplates) {
        this.inputTransactionTemplates = inputTransactionTemplates;
    }

    public String getOutputTransactionTemplate() {
        return outputTransactionTemplate;
    }

    public void setOutputTransactionTemplate(String outputTransactionTemplate) {
        this.outputTransactionTemplate = outputTransactionTemplate;
    }

    public Long getTransactionCounts() {
        return transactionCounts;
    }

    public void setTransactionCounts(Long transactionCounts) {
        this.transactionCounts = transactionCounts;
    }

    public Long getTransactionActionId() {
        return transactionActionId;
    }

    public void setTransactionActionId(Long transactionActionId) {
        this.transactionActionId = transactionActionId;
    }
}
