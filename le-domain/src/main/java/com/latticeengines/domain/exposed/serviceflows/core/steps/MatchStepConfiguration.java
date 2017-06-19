package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.MatchJoinType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

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

    @JsonProperty("match_hdfs_pod")
    private String matchHdfsPod = "";

    private boolean retainMatchTables;

    private boolean excludeUnmatchedWithPublicDomain = false;
    private boolean publicDomainAsNormalDomain = false;

    @NotNull
    private MatchCommandType matchCommandType;

    private MatchJoinType matchJoinType = MatchJoinType.OUTER_JOIN;

    @NotEmptyString
    @NotNull
    private String inputTableName;

    private Predefined predefinedColumnSelection;
    private String predefinedSelectionVersion;

    private String dataCloudVersion;

    private ColumnSelection customizedColumnSelection;

    private String matchQueue;

    private String sourceSchemaInterpretation;

    @JsonProperty("skip_dedupe")
    private boolean skipDedupe;

    @JsonProperty("match_request_source")
    private MatchRequestSource matchRequestSource;

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

    @JsonProperty("exclude_unmatched_public_domain")
    public boolean isExcludeUnmatchedWithPublicDomain() {
        return excludeUnmatchedWithPublicDomain;
    }

    @JsonProperty("exclude_unmatched_public_domain")
    public void setExcludeUnmatchedWithPublicDomain(boolean excludeUnmatchedWithPublicDomain) {
        this.excludeUnmatchedWithPublicDomain = excludeUnmatchedWithPublicDomain;
    }

    @JsonProperty("public_domain_as_normal_domain")
    public boolean isPublicDomainAsNormalDomain() {
        return publicDomainAsNormalDomain;
    }

    @JsonProperty("public_domain_as_normal_domain")
    public void setPublicDomainAsNormalDomain(boolean publicDomainAsNormalDomain) {
        this.publicDomainAsNormalDomain = publicDomainAsNormalDomain;
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
    public Predefined getPredefinedColumnSelection() {
        return predefinedColumnSelection;
    }

    @JsonProperty("predefined_col_selection")
    public void setPredefinedColumnSelection(Predefined predefinedColumnSelection) {
        this.predefinedColumnSelection = predefinedColumnSelection;
    }

    @JsonProperty("predefined_selection_version")
    public String getPredefinedSelectionVersion() {
        return predefinedSelectionVersion;
    }

    @JsonProperty("data_cloud_version")
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    @JsonProperty("data_cloud_version")
    public void setDataCloudVersion(String dataCloudVersion) {
          this.dataCloudVersion = dataCloudVersion;
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

    @JsonProperty("source_schema_interpretation")
    public String getSourceSchemaInterpretation() {
        return sourceSchemaInterpretation;
    }

    @JsonProperty("source_schema_interpretation")
    public void setSourceSchemaInterpretation(String sourceSchemaInterpretation) {
        this.sourceSchemaInterpretation = sourceSchemaInterpretation;
    }

    public String getMatchHdfsPod() {
        return matchHdfsPod;
    }

    public void setMatchHdfsPod(String matchHdfsPod) {
        this.matchHdfsPod = matchHdfsPod;
    }

    public boolean isSkipDedupe() {
        return skipDedupe;
    }

    public void setSkipDedupe(boolean skipDedupe) {
        this.skipDedupe = skipDedupe;
    }

    public void setMatchRequestSource(MatchRequestSource matchRequestSource) {
        this.matchRequestSource = matchRequestSource;
    }

    public MatchRequestSource getMatchRequestSource() {
        return matchRequestSource;
    }
    
}
