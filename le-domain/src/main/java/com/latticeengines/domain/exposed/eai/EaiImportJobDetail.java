package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import org.hibernate.annotations.Index;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.io.Serializable;
import java.util.Date;

@Entity
@javax.persistence.Table(name = "EAI_IMPORT_JOB_DETAIL")
public class EaiImportJobDetail implements HasPid, Serializable {

    private static final long serialVersionUID = -1299972365295269629L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @JsonProperty("pid")
    private Long pid;

    @Column(name = "COLLECTION_IDENTIFIER", unique = true, nullable = false)
    @Index(name = "IX_COLLECTION_IDENTIFIER")
    @JsonProperty("collection_identifier")
    private String collectionIdentifier;

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
}
