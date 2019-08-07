package com.latticeengines.domain.exposed.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ImportMigrateReport {

    @JsonProperty("system_name")
    private String systemName;

    @JsonProperty("input_account_templates")
    private List<String> inputAccountTemplates;

    @JsonProperty("output_account_template")
    private String outputAccountTemplate;

    @JsonProperty("output_account_task_id")
    private String outputAccountTaskId;

    @JsonProperty("account_counts")
    private Long accountCounts;

    @JsonProperty("account_action_id")
    private Long accountActionId;

    @JsonProperty("account_data_tables")
    private List<String> accountDataTables;

    @JsonProperty("input_contact_templates")
    private List<String> inputContactTemplates;

    @JsonProperty("output_contact_template")
    private String outputContactTemplate;

    @JsonProperty("output_contact_task_id")
    private String outputContactTaskId;

    @JsonProperty("contact_counts")
    private Long contactCounts;

    @JsonProperty("contact_action_id")
    private Long contactActionId;

    @JsonProperty("contact_data_tables")
    private List<String> contactDataTables;

    @JsonProperty("input_transaction_templates")
    private List<String> inputTransactionTemplates;

    @JsonProperty("output_transaction_template")
    private String outputTransactionTemplate;

    @JsonProperty("output_transaction_task_id")
    private String outputTransactionTaskId;

    @JsonProperty("transaction_counts")
    private Long transactionCounts;

    @JsonProperty("transaction_action_id")
    private Long transactionActionId;

    @JsonProperty("transaction_data_tables")
    private List<String> transactionDataTables;

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

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

    public String getOutputAccountTaskId() {
        return outputAccountTaskId;
    }

    public void setOutputAccountTaskId(String outputAccountTaskId) {
        this.outputAccountTaskId = outputAccountTaskId;
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

    public List<String> getAccountDataTables() {
        return accountDataTables;
    }

    public void setAccountDataTables(List<String> accountDataTables) {
        this.accountDataTables = accountDataTables;
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

    public String getOutputContactTaskId() {
        return outputContactTaskId;
    }

    public void setOutputContactTaskId(String outputContactTaskId) {
        this.outputContactTaskId = outputContactTaskId;
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

    public List<String> getContactDataTables() {
        return contactDataTables;
    }

    public void setContactDataTables(List<String> contactDataTables) {
        this.contactDataTables = contactDataTables;
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

    public String getOutputTransactionTaskId() {
        return outputTransactionTaskId;
    }

    public void setOutputTransactionTaskId(String outputTransactionTaskId) {
        this.outputTransactionTaskId = outputTransactionTaskId;
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

    public List<String> getTransactionDataTables() {
        return transactionDataTables;
    }

    public void setTransactionDataTables(List<String> transactionDataTables) {
        this.transactionDataTables = transactionDataTables;
    }
}
