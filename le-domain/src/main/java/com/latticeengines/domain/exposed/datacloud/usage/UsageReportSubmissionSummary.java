package com.latticeengines.domain.exposed.datacloud.usage;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class UsageReportSubmissionSummary {

    @JsonProperty("reports")
    private Map<Group, List<Report>> reports;

    public Map<Group, List<Report>> getReports() {
        return reports;
    }

    public void setReports(Map<Group, List<Report>> reports) {
        this.reports = reports;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect( //
            fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE //
    )
    public static class Report {
        @JsonProperty("reportId")
        private String reportId;

        @JsonProperty("numRecords")
        private Long numRecords;

        @JsonProperty("createdAt")
        private Date createdAt;

        @JsonProperty("submittedAt")
        private Date submittedAt;

        public String getReportId() {
            return reportId;
        }

        public void setReportId(String reportId) {
            this.reportId = reportId;
        }

        public Long getNumRecords() {
            return numRecords;
        }

        public void setNumRecords(Long numRecords) {
            this.numRecords = numRecords;
        }

        public Date getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(Date createdAt) {
            this.createdAt = createdAt;
        }

        public Date getSubmittedAt() {
            return submittedAt;
        }

        public void setSubmittedAt(Date submittedAt) {
            this.submittedAt = submittedAt;
        }
    }

    public enum Group {
        PENDING("New reports to be moved to VBO later"),
        MOVE("Reports just moved to VBO in this request"),
        PREVIOUS("Reports previously moved to VBO that might be in process");

        private final String description;
        Group(String displayName) {
            this.description = displayName;
        }
        @JsonValue
        public String getDescription() {
            return description;
        }

    }

}
