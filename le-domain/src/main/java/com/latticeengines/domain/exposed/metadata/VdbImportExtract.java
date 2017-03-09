package com.latticeengines.domain.exposed.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import org.hibernate.annotations.Index;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.io.Serializable;
import java.util.Date;

@Entity
@javax.persistence.Table(name = "METADATA_VDB_EXTRACT")
public class VdbImportExtract implements HasPid, Serializable {

    private static final long serialVersionUID = -563028614323958857L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @JsonProperty("pid")
    private Long pid;

    @Column(name = "EXTRACT_IDENTIFIER", unique = true, nullable = false)
    @Index(name = "IX_EXTRACT_IDENTIFIER")
    @JsonProperty("extract_identifier")
    private String extractIdentifier;

    @Column(name = "EXTRACTION_TS", nullable = false)
    @JsonProperty("extraction_ts")
    private Date extractionTimestamp;

    @Column(name = "LOAD_APPLICATION_ID")
    @JsonProperty("load_application_id")
    private String loadApplicationId;

    @Column(name = "IMPORT_STATUS", nullable = false)
    @JsonProperty("vdb_import_status")
    private VdbImportStatus status;

    @Column(name = "PROCESSED_RECORDS", nullable = false)
    @JsonProperty("processed_records")
    private int processedRecords;

    @Column(name = "LINES_PER_FILE")
    @JsonProperty("lines_per_file")
    private int linesPerFile;

    @Column(name = "TARGET_PATH", length = 2048)
    @JsonProperty("target_path")
    private String targetPath;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getExtractIdentifier() {
        return extractIdentifier;
    }

    public void setExtractIdentifier(String extractIdentifier) {
        this.extractIdentifier = extractIdentifier;
    }

    public Date getExtractionTimestamp() {
        return extractionTimestamp;
    }

    public void setExtractionTimestamp(Date extractionTimestamp) {
        this.extractionTimestamp = extractionTimestamp;
    }

    public String getLoadApplicationId() {
        return loadApplicationId;
    }

    public void setLoadApplicationId(String loadApplicationId) {
        this.loadApplicationId = loadApplicationId;
    }

    public VdbImportStatus getStatus() {
        return status;
    }

    public void setStatus(VdbImportStatus status) {
        this.status = status;
    }

    public int getProcessedRecords() {
        return processedRecords;
    }

    public void setProcessedRecords(int processedRecords) {
        this.processedRecords = processedRecords;
    }

    public int getLinesPerFile() {
        return linesPerFile;
    }

    public void setLinesPerFile(int linesPerFile) {
        this.linesPerFile = linesPerFile;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }
}
