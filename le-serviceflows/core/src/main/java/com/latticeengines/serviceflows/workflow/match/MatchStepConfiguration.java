package com.latticeengines.serviceflows.workflow.match;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.propdata.MatchJoinType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class MatchStepConfiguration extends MicroserviceStepConfiguration {

    @NotEmptyString
    @NotNull
    private String dbUrl;

    @NotEmptyString
    @NotNull
    private String dbUser;

    @NotEmptyString
    @NotNull
    private String dbPasswordEncrypted;

    @NotEmptyString
    @NotNull
    private String destTables;

    @NotEmptyString
    @NotNull
    private String matchClient;

    private boolean retainMatchTables;

    @NotNull
    private MatchCommandType matchCommandType;

    private MatchJoinType matchJoinType = MatchJoinType.OUTER_JOIN;

    @NotEmptyString
    @NotNull
    private String inputTableName;

    private ColumnSelection.Predefined predefinedColumnSelection;
    private String predefinedSelectionVersion;

    private ColumnSelection customizedColumnSelection;

    private String matchQueue = LedpQueueAssigner.getModelingQueueNameForSubmission();

    @JsonProperty("db_url")
    public String getDbUrl() {
        return dbUrl;
    }

    @JsonProperty("db_url")
    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    @JsonProperty("db_user")
    public String getDbUser() {
        return dbUser;
    }

    @JsonProperty("db_user")
    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    @JsonProperty("db_password_encrypted")
    public String getDbPasswordEncrypted() {
        return dbPasswordEncrypted;
    }

    @JsonProperty("db_password_encrypted")
    public void setDbPasswordEncrypted(String dbPassword) {
        this.dbPasswordEncrypted = dbPassword;
    }

    @JsonProperty("dest_tables")
    public String getDestTables() {
        return destTables;
    }

    @JsonProperty("dest_tables")
    public void setDestTables(String destTables) {
        this.destTables = destTables;
    }

    @JsonProperty("match_client")
    public String getMatchClient() {
        return matchClient;
    }

    @JsonProperty("match_client")
    public void setMatchClient(String matchClient) {
        this.matchClient = matchClient;
    }

    @JsonProperty("command_type")
    public MatchCommandType getMatchCommandType() {
        return matchCommandType;
    }

    @JsonProperty("command_type")
    public void setMatchCommandType(MatchCommandType matchCommandType) {
        this.matchCommandType = matchCommandType;
    }

    @JsonProperty("input_table_name")
    public String getInputTableName() {
        return inputTableName;
    }

    @JsonProperty("input_table_name")
    public void setInputTableName(String inputTableName) {
        this.inputTableName = inputTableName;
    }

    @JsonProperty("join_type")
    public MatchJoinType getMatchJoinType() {
        return matchJoinType;
    }

    @JsonProperty("join_type")
    public void setMatchJoinType(MatchJoinType matchJoinType) {
        this.matchJoinType = matchJoinType;
    }

    @JsonProperty("retain_match_tables")
    public boolean isRetainMatchTables() {
        return retainMatchTables;
    }

    @JsonProperty("retain_match_tables")
    public void setRetainMatchTables(boolean retainMatchTables) {
        this.retainMatchTables = retainMatchTables;
    }

    @JsonProperty("match_queue")
    public String getMatchQueue() {
        return matchQueue;
    }

    @JsonProperty("match_queue")
    public void setMatchQueue(String matchQueue) {
        this.matchQueue = matchQueue;
    }

    @JsonProperty("predefined_col_selection")
    public ColumnSelection.Predefined getPredefinedColumnSelection() {
        return predefinedColumnSelection;
    }

    @JsonProperty("predefined_col_selection")
    public void setPredefinedColumnSelection(ColumnSelection.Predefined predefinedColumnSelection) {
        this.predefinedColumnSelection = predefinedColumnSelection;
    }

    @JsonProperty("predefined_selection_version")
    public String getPredefinedSelectionVersion() {
        return predefinedSelectionVersion;
    }

    @JsonProperty("predefined_selection_version")
    public void setPredefinedSelectionVersion(String predefinedSelectionVersion) {
        this.predefinedSelectionVersion = predefinedSelectionVersion;
    }

    @JsonProperty("customized_col_selection")
    public ColumnSelection getCustomizedColumnSelection() {
        return customizedColumnSelection;
    }

    @JsonProperty("customized_col_selection")
    public void setCustomizedColumnSelection(ColumnSelection customizedColumnSelection) {
        this.customizedColumnSelection = customizedColumnSelection;
    }
}
