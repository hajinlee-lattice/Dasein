package com.latticeengines.domain.exposed.propdata.match;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchOutput {

    private List<String> inputFields;
    private Map<MatchKey, String> keyMap;
    private List<String> outputFields;
    private List<OutputRecord> result;
    private List<ColumnMetadata> metadata;
    private MatchStatistics statistics;
    private Tenant submittedBy;
    private Date receivedAt;
    private Date finishedAt;
    private String rootOperationUID = UUID.randomUUID().toString().toUpperCase();

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
    private static Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    static {
        formatter.setCalendar(calendar);
    }

    @JsonProperty("InputFields")
    public List<String> getInputFields() {
        return inputFields;
    }

    @JsonProperty("InputFields")
    public void setInputFields(List<String> inputFields) {
        this.inputFields = inputFields;
    }

    @JsonProperty("OutputFields")
    public List<String> getOutputFields() {
        return outputFields;
    }

    @JsonProperty("OutputFields")
    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }

    @JsonProperty("KeyMap")
    public Map<MatchKey, String> getKeyMap() {
        return keyMap;
    }

    @JsonProperty("KeyMap")
    public void setKeyMap(Map<MatchKey, String> keyMap) {
        this.keyMap = keyMap;
    }

    @JsonProperty("Result")
    public List<OutputRecord> getResult() {
        return result;
    }

    @JsonProperty("Result")
    public void setResult(List<OutputRecord> result) {
        this.result = result;
    }

    @JsonProperty("Metadata")
    public List<ColumnMetadata> getMetadata() {
        return metadata;
    }

    @JsonProperty("Metadata")
    public void setMetadata(List<ColumnMetadata> metadata) {
        this.metadata = metadata;
    }

    @JsonProperty("Statistics")
    public MatchStatistics getStatistics() {
        return statistics;
    }

    @JsonProperty("Statistics")
    public void setStatistics(MatchStatistics statistics) {
        this.statistics = statistics;
    }

    @JsonProperty("SubmittedBy")
    public Tenant getSubmittedBy() {
        return submittedBy;
    }

    @JsonProperty("SubmittedBy")
    public void setSubmittedBy(Tenant submittedBy) {
        this.submittedBy = submittedBy;
    }

    @JsonIgnore
    public Date getReceivedAt() {
        return receivedAt;
    }

    @JsonIgnore
    public void setReceivedAt(Date receivedAt) {
        this.receivedAt = receivedAt;
    }

    @JsonProperty("ReceivedAt")
    private String getReceivedAtAsString() {
        return receivedAt == null ? null : formatter.format(receivedAt);
    }

    @JsonProperty("ReceivedAt")
    private void setReceivedAtByString(String requestSubmittedAt) {
        try {
            this.receivedAt = formatter.parse(requestSubmittedAt);
        } catch (ParseException e) {
            this.receivedAt = null;
        }
    }

    @JsonIgnore
    public Date getFinishedAt() {
        return finishedAt;
    }

    @JsonIgnore
    public void setFinishedAt(Date finishedAt) {
        this.finishedAt = finishedAt;
    }

    @JsonProperty("FinishedAt")
    private String getFinishedAtAsString() {
        return finishedAt == null ? null : formatter.format(finishedAt);
    }

    @JsonProperty("FinishedAt")
    private void setFinishedAtByString(String resultGeneratedAt) {
        try {
            this.finishedAt = formatter.parse(resultGeneratedAt);
        } catch (ParseException e) {
            this.finishedAt = null;
        }
    }

    @JsonProperty("RootOperationUID")
    public String getRootOperationUID() {
        return rootOperationUID;
    }

    @JsonProperty("RootOperationUID")
    public void setRootOperationUID(String rootOperationUID) {
        this.rootOperationUID = rootOperationUID;
    }

}
