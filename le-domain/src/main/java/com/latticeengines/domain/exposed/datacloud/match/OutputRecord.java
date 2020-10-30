package com.latticeengines.domain.exposed.datacloud.match;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.MatchCoreErrorConstants;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OutputRecord {

    @JsonProperty("RowNumber")
    private Integer rowNumber;

    @JsonProperty("IsMatched")
    private boolean matched;

    @JsonProperty("Input")
    private List<Object> input;

    @JsonProperty("PreMatchDomain")
    private String preMatchDomain;

    @JsonProperty("PreMatchNameLocation")
    private NameLocation preMatchNameLocation;

    @JsonProperty("PreMatchDUNS")
    private String preMatchDuns;

    @JsonProperty("PreMatchEmail")
    private String preMatchEmail;

    @JsonProperty("LatticeAccountId")
    private String matchedLatticeAccountId;

    @JsonProperty("MatchedDomain")
    private String matchedDomain;

    @JsonProperty("MatchedNameLocation")
    private NameLocation matchedNameLocation;

    @JsonProperty("MatchedDUNS")
    private String matchedDuns;

    @JsonProperty("MatchedEmail")
    private String matchedEmail;

    @JsonProperty("MatchedDDUNS")
    private String matchedDduns;

    @JsonProperty("MatcheEmployeeRange")
    private String matchedEmployeeRange;
    @JsonProperty("MatchedRevenueRange")
    private String matchedRevenueRange;
    @JsonProperty("MatchedPrimaryIndustry")
    private String matchedPrimaryIndustry;
    @JsonProperty("MatchedSecondIndustry")
    private String matchedSecondIndustry;
    @JsonProperty("DomainSource")
    private String domainSource;

    @JsonProperty("Output")
    private List<Object> output; // single-result output

    @JsonProperty("Candidates")
    private List<List<Object>> candidateOutput; // also use as multi-result output

    @JsonProperty("UsageEvents")
    private List<VboUsageEvent> usageEvents;

    @JsonProperty("MatchLogs")
    private List<String> matchLogs;

    @JsonProperty("ErrorMessages")
    private List<String> errorMessages;

    @JsonProperty("ErrorCodes")
    private Map<MatchCoreErrorConstants.ErrorType, List<String>> errorCodes;

    @JsonProperty("DebugValues")
    private List<String> debugValues;

    @JsonIgnore
    private List<String> dnbCacheIds;

    // Newly Allocated EntityIDs: entity -> entityId
    @JsonIgnore
    private Map<String, String> newEntityIds;

    @JsonProperty("NumFeatureValue")
    private int numFeatureValue;

    public Integer getRowNumber() {
        return rowNumber;
    }

    public void setRowNumber(Integer rowNumber) {
        this.rowNumber = rowNumber;
    }

    public boolean isMatched() {
        return matched;
    }

    public void setMatched(boolean matched) {
        this.matched = matched;
    }

    public List<Object> getInput() {
        return input;
    }

    public void setInput(List<Object> input) {
        this.input = input;
    }

    public String getPreMatchDomain() {
        return preMatchDomain;
    }

    public void setPreMatchDomain(String preMatchDomain) {
        this.preMatchDomain = preMatchDomain;
    }

    public NameLocation getPreMatchNameLocation() {
        return preMatchNameLocation;
    }

    public void setPreMatchNameLocation(NameLocation preMatchNameLocation) {
        this.preMatchNameLocation = preMatchNameLocation;
    }

    public String getPreMatchDuns() {
        return preMatchDuns;
    }

    public void setPreMatchDuns(String preMatchDuns) {
        this.preMatchDuns = preMatchDuns;
    }

    public String getPreMatchEmail() {
        return preMatchEmail;
    }

    public void setPreMatchEmail(String preMatchEmail) {
        this.preMatchEmail = preMatchEmail;
    }

    public String getMatchedLatticeAccountId() {
        return matchedLatticeAccountId;
    }

    public void setMatchedLatticeAccountId(String matchedLatticeAccountId) {
        this.matchedLatticeAccountId = matchedLatticeAccountId;
    }

    public String getMatchedDomain() {
        return matchedDomain;
    }

    public void setMatchedDomain(String matchedDomain) {
        this.matchedDomain = matchedDomain;
    }

    public NameLocation getMatchedNameLocation() {
        return matchedNameLocation;
    }

    public void setMatchedNameLocation(NameLocation matchedNameLocation) {
        this.matchedNameLocation = matchedNameLocation;
    }

    public String getMatchedDuns() {
        return matchedDuns;
    }

    public void setMatchedDuns(String matchedDuns) {
        this.matchedDuns = matchedDuns;
    }

    public String getMatchedDduns() {
        return matchedDduns;
    }

    public void setMatchedDduns(String matchedDduns) {
        this.matchedDduns = matchedDduns;
    }

    public String getMatchedEmail() {
        return matchedEmail;
    }

    public void setMatchedEmail(String matchedEmail) {
        this.matchedEmail = matchedEmail;
    }

    public List<Object> getOutput() {
        return output;
    }

    public void setOutput(List<Object> output) {
        this.output = output;
    }

    public List<List<Object>> getCandidateOutput() {
        return candidateOutput;
    }

    public void setCandidateOutput(List<List<Object>> candidateOutput) {
        this.candidateOutput = candidateOutput;
    }

    public List<VboUsageEvent> getUsageEvents() {
        return usageEvents;
    }

    public void setUsageEvents(List<VboUsageEvent> usageEvents) {
        this.usageEvents = usageEvents;
    }

    public List<String> getMatchLogs() {
        return matchLogs;
    }

    public void setMatchLogs(List<String> matchLogs) {
        this.matchLogs = matchLogs;
    }

    public List<String> getErrorMessages() {
        return errorMessages;
    }

    public void setErrorMessages(List<String> errorMessages) {
        this.errorMessages = errorMessages;
    }

    public void addErrorMessages(String errorMessage) {
        if (this.errorMessages == null) {
            this.errorMessages = new ArrayList<>();
        }
        this.errorMessages.add(errorMessage);
    }

    public Map<MatchCoreErrorConstants.ErrorType, List<String>> getErrorCodes() {
        return errorCodes;
    }

    public void setErrorCodes(Map<MatchCoreErrorConstants.ErrorType, List<String>> errorCodes) {
        this.errorCodes = errorCodes;
    }

    public void addErrorCode(MatchCoreErrorConstants.ErrorType type, String code) {
        if (this.errorCodes == null) {
            this.errorCodes = new HashMap<>();
        }

        if (!this.errorCodes.containsKey(type)) {
            this.errorCodes.put(type, new ArrayList<>());
        }

        this.errorCodes.get(type).add(code);
    }

    public List<String> getDebugValues() {
        return debugValues;
    }

    public void setDebugValues(List<String> debugValues) {
        this.debugValues = debugValues;
    }

    public List<String> getDnbCacheIds() {
        return dnbCacheIds;
    }

    public void setDnbCacheIds(List<String> dnbCacheIds) {
        this.dnbCacheIds = dnbCacheIds;
    }

    public Map<String, String> getNewEntityIds() {
        return newEntityIds;
    }

    public void setNewEntityIds(Map<String, String> newEntityIds) {
        this.newEntityIds = newEntityIds;
    }

    public String getMatchedEmployeeRange() {
        return matchedEmployeeRange;
    }

    public void setMatchedEmployeeRange(String matchedEmployeeRange) {
        this.matchedEmployeeRange = matchedEmployeeRange;
    }

    public String getMatchedRevenueRange() {
        return matchedRevenueRange;
    }

    public void setMatchedRevenueRange(String matchedRevenueRange) {
        this.matchedRevenueRange = matchedRevenueRange;
    }

    public String getMatchedPrimaryIndustry() {
        return matchedPrimaryIndustry;
    }

    public void setMatchedPrimaryIndustry(String matchedPrimaryIndustry) {
        this.matchedPrimaryIndustry = matchedPrimaryIndustry;
    }

    public String getMatchedSecondIndustry() {
        return matchedSecondIndustry;
    }

    public void setMatchedSecondIndustry(String matchedSecondIndustry) {
        this.matchedSecondIndustry = matchedSecondIndustry;
    }

    public String getDomainSource() {
        return domainSource;
    }

    public void setDomainSource(String domainSource) {
        this.domainSource = domainSource;
    }

    public void log(String log) {
        if (this.matchLogs == null) {
            matchLogs = new ArrayList<>();
        }
        matchLogs.add(log);
    }

    public int getNumFeatureValue() {
        return numFeatureValue;
    }

    public void setNumFeatureValue(int numFeatureValue) {
        this.numFeatureValue = numFeatureValue;
    }

}
