package com.latticeengines.serviceflows.workflow.match;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.propdata.MatchJoinType;
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

    @NotNull
    private MatchCommandType matchCommandType;

    private MatchJoinType matchJoinType = MatchJoinType.INNER_JOIN;

    @NotEmptyString
    @NotNull
    private String inputTableName;

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
}
