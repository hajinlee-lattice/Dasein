package com.latticeengines.domain.exposed.datacloud.match;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchOutput {

    private static Log log = LogFactory.getLog(MatchOutput.class);

    private List<String> inputFields;
    private Map<MatchKey, List<String>> keyMap;
    private List<String> outputFields;
    private List<OutputRecord> result;
    private List<ColumnMetadata> metadata;
    private MatchStatistics statistics;
    private Tenant submittedBy;
    private Date receivedAt;
    private Date finishedAt;
    private String rootOperationUID;

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS z";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
    private static Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    // for json constructor
    @SuppressWarnings("unused")
    private MatchOutput() {}

    public MatchOutput(UUID uuid) {
        this.rootOperationUID = uuid.toString().toUpperCase();
    }

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
    public Map<MatchKey, List<String>> getKeyMap() {
        return keyMap;
    }

    @JsonProperty("KeyMap")
    public void setKeyMap(Map<MatchKey, List<String>> keyMap) {
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

    @MetricFieldGroup
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
            SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
            formatter.setCalendar(calendar);
            this.receivedAt = formatter.parse(requestSubmittedAt);
        } catch (Exception e) {
            log.error("Failed to parse timestamp ReceviedAt [" + requestSubmittedAt + "]", e);
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
            SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
            formatter.setCalendar(calendar);
            this.finishedAt = formatter.parse(resultGeneratedAt);
        } catch (Exception e) {
            log.error("Failed to parse timestamp FinishedAt [" + resultGeneratedAt + "]", e);
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

    @MetricField(name = "OutputFields", fieldType = MetricField.FieldType.INTEGER)
    public Integer numOutputFields() {
        return getOutputFields().size();
    }
}
