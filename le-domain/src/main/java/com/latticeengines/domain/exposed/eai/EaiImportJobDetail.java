package com.latticeengines.domain.exposed.eai;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Lob;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@javax.persistence.Table(name = "EAI_IMPORT_JOB_DETAIL", indexes = {
        @Index(name = "IX_COLLECTION_IDENTIFIER", columnList = "COLLECTION_IDENTIFIER"),
        @Index(name = "IX_SEQUENCE_ID", columnList = "SEQUENCE_ID") }, uniqueConstraints = {
                @UniqueConstraint(columnNames = { "COLLECTION_IDENTIFIER", "SEQUENCE_ID" }) })
public class EaiImportJobDetail implements HasPid, Serializable {

    private static final long serialVersionUID = -1299972365295269629L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @JsonProperty("pid")
    private Long pid;

    @Column(name = "COLLECTION_IDENTIFIER", nullable = false)
    @JsonProperty("collection_identifier")
    private String collectionIdentifier;

    @Column(name = "SEQUENCE_ID", nullable = false)
    @JsonProperty("sequence_id")
    private Long sequenceId;

    @Column(name = "COLLECTION_TS", nullable = false)
    @JsonProperty("collection_ts")
    private Date collectionTimestamp;

    @Column(name = "LOAD_APPLICATION_ID")
    @JsonProperty("load_application_id")
    private String loadApplicationId;

    @Column(name = "IMPORT_STATUS", nullable = false)
    @JsonProperty("import_status")
    @Enumerated(EnumType.STRING)
    private ImportStatus status;

    @Column(name = "SOURCE_TYPE", nullable = false)
    @JsonProperty("source_type")
    @Enumerated(EnumType.STRING)
    private SourceType sourceType;

    @Column(name = "PROCESSED_RECORDS", nullable = false)
    @JsonProperty("processed_records")
    private int processedRecords;

    @Column(name = "TARGET_PATH", length = 2048)
    @JsonProperty("target_path")
    private String targetPath;

    @Column(name = "DETAILS", nullable = false)
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    private Map<String, Object> details = new HashMap<>();

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getCollectionIdentifier() {
        return collectionIdentifier;
    }

    public void setCollectionIdentifier(String collectionIdentifier) {
        this.collectionIdentifier = collectionIdentifier;
    }

    public Long getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(Long sequenceId) {
        this.sequenceId = sequenceId;
    }

    public Date getCollectionTimestamp() {
        return collectionTimestamp;
    }

    public void setCollectionTimestamp(Date collectionTimestamp) {
        this.collectionTimestamp = collectionTimestamp;
    }

    public String getLoadApplicationId() {
        return loadApplicationId;
    }

    public void setLoadApplicationId(String loadApplicationId) {
        this.loadApplicationId = loadApplicationId;
    }

    public ImportStatus getStatus() {
        return status;
    }

    public void setStatus(ImportStatus status) {
        this.status = status;
    }

    public SourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(SourceType sourceType) {
        this.sourceType = sourceType;
    }

    public int getProcessedRecords() {
        return processedRecords;
    }

    public void setProcessedRecords(int processedRecords) {
        this.processedRecords = processedRecords;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    public Map<String, Object> getDetails() {
        return details;
    }

    public void setDetails(Map<String, Object> details) {
        this.details = details;
    }

    @Transient
    @JsonIgnore
    public void setPRDetail(List<String> recordList) {
        setDetailValue("ProcessedRecordsList", recordList);
    }

    @Transient
    @JsonIgnore
    @SuppressWarnings("unchecked")
    public List<String> getPRDetail() {
        return (List<String>) details.get("ProcessedRecordsList");
    }

    @Transient
    @JsonIgnore
    public void setPRDetail(String records) {
        setListDetailFromString("ProcessedRecordsList", records.toString());
    }

    @Transient
    @JsonIgnore
    public void setPathDetail(String pathList) {
        setListDetailFromString("ExtractPathList", pathList);
    }

    @Transient
    @JsonIgnore
    @SuppressWarnings("unchecked")
    public List<String> getPathDetail() {
        return (List<String>) details.get("ExtractPathList");
    }

    @Transient
    @JsonIgnore
    public void setPathDetail(List<String> pathList) {
        setDetailValue("ExtractPathList", pathList);
    }

    @Transient
    @JsonIgnore
    public String getTemplateName() {
        return getDetailValue("TemplateName") != null ? getDetailValue("TemplateName").toString()
                : null;
    }

    @Transient
    @JsonIgnore
    public void setTemplateName(String templateName) {
        setDetailValue("TemplateName", templateName);
    }

    @Transient
    @JsonIgnore
    public String getImportFileName() {
        return getDetailValue("ImportFileName") != null
                ? getDetailValue("ImportFileName").toString() : null;
    }

    @Transient
    @JsonIgnore
    public void setImportFileName(String importFileName) {
        setDetailValue("ImportFileName", importFileName);
    }

    @Transient
    @JsonIgnore
    public void setDetailValue(String key, Object value) {
        details.put(key, value);
    }

    @Transient
    @JsonIgnore
    public Object getDetailValue(String key) {
        return details.get(key);
    }

    @Transient
    @JsonIgnore
    public String getReportURL() {
        return getDetailValue("ReportURL") != null ? getDetailValue("ReportURL").toString() : null;
    }

    @Transient
    @JsonIgnore
    public void setReportURL(String url) {
        setDetailValue("ReportURL", url);
    }

    @Transient
    @JsonIgnore
    public String getQueryHandle() {
        return getDetailValue("QueryHandle") != null ? getDetailValue("QueryHandle").toString()
                : null;
    }

    @Transient
    @JsonIgnore
    public void setQueryHandle(String queryHandle) {
        setDetailValue("QueryHandle", queryHandle);
    }

    @Transient
    @JsonIgnore
    public Long getTotalRows() {
        return getDetailValue("TotalRows") != null
                ? Long.parseLong(getDetailValue("TotalRows").toString()) : null;
    }

    @Transient
    @JsonIgnore
    public void setTotalRows(Long totalRows) {
        setDetailValue("TotalRows", totalRows);
    }

    @Transient
    @JsonIgnore
    public Long getIgnoredRows() {
        return getDetailValue("IgnoredRows") != null
                ? Long.parseLong(getDetailValue("IgnoredRows").toString()) : null;
    }

    @Transient
    @JsonIgnore
    public void setIgnoredRows(Long ignoredRows) {
        setDetailValue("IgnoredRows", ignoredRows);
    }

    @Transient
    @JsonIgnore
    public Long getDedupedRows() {
        return getDetailValue("DedupedRows") != null
                ? Long.parseLong(getDetailValue("DedupedRows").toString()) : null;
    }

    @Transient
    @JsonIgnore
    public void setDedupedRows(Long dedupedRows) {
        setDetailValue("DedupedRows", dedupedRows);
    }

    @Transient
    @JsonIgnore
    private void setListDetailFromString(String key, String value) {
        Pattern pattern = Pattern.compile("^\\[(.*)\\]$");
        if (value != null) {
            Matcher matcher = pattern.matcher(value);
            if (matcher.matches()) {
                String contents = matcher.group(1);
                if (contents.isEmpty()) {
                    setDetailValue(key, new ArrayList<>());
                } else {
                    String[] array = contents.split(",");
                    for (int i = 0; i < array.length; ++i) {
                        array[i] = array[i].trim();
                    }
                    setDetailValue(key, Arrays.asList(array));
                }
            } else {
                setDetailValue(key, Arrays.asList(value));
            }
        } else {
            setDetailValue(key, Arrays.asList(value));
        }
    }
}
