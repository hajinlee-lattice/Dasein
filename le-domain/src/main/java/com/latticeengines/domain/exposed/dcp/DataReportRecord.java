package com.latticeengines.domain.exposed.dcp;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Persist object for Dashboard
 */
@Entity
@Table(name = "DCP_DATA_REPORT",
        indexes = {
                @Index(name = "IX_OWNER_ID_LEVEL", columnList = "OWNER_ID,LEVEL"),
                @Index(name = "IX_PARENT_ID", columnList = "PARENT_ID")},
        uniqueConstraints = {
                @UniqueConstraint(name = "IX_ID_LEVEL", columnNames = {"FK_TENANT_ID", "OWNER_ID", "LEVEL"})})
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class DataReportRecord implements HasPid, HasTenant, HasAuditingFields {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = {CascadeType.MERGE}, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @Column(name = "PARENT_ID")
    private Long parentId;

    @Column(name = "OWNER_ID", nullable = false)
    private String ownerId;

    @Column(name = "LEVEL", length = 20)
    @Enumerated(EnumType.STRING)
    private Level level;

    @Column(name = "DATA_SNAPSHOT_TIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date dataSnapshotTime;

    @ManyToOne(cascade = {CascadeType.MERGE}, fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_DUNS_COUNT")
    private com.latticeengines.domain.exposed.metadata.Table dunsCount;

    @Column(name = "REFRESH_TIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date refreshTime;

    @Column(name = "BASIC_STATS", columnDefinition = "'JSON'")
    @Type(type = "json")
    private DataReport.BasicStats basicStats;

    @Column(name = "INPUT_PRESENCE_REPORT", columnDefinition = "'JSON'")
    @Type(type = "json")
    private DataReport.InputPresenceReport inputPresenceReport;

    @Column(name = "GEO_DISTRIBUTION_REPORT", columnDefinition = "'JSON'")
    @Type(type = "json")
    private DataReport.GeoDistributionReport geoDistributionReport;

    @Column(name = "MATCH_TO_DUNS_REPORT", columnDefinition = "'JSON'")
    @Type(type = "json")
    private DataReport.MatchToDUNSReport matchToDUNSReport;

    @Column(name = "DUPLICATION_REPORT", columnDefinition = "'JSON'")
    @Type(type = "json")
    private DataReport.DuplicationReport duplicationReport;

    // this is used to indicate the data report
    // is ready for roll up
    @Column(name = "READY_FOR_ROLLUP", nullable = false)
    private boolean readyForRollup;

    @Column(name = "ROLLUP_STATUS", nullable = false)
    @Enumerated(EnumType.STRING)
    private RollupStatus rollupStatus = RollupStatus.READY;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date date) {
        this.created = date;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date date) {
        this.updated = date;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public Long getParentId() {
        return parentId;
    }

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    public Level getLevel() {
        return level;
    }

    public void setLevel(Level level) {
        this.level = level;
    }

    public Date getDataSnapshotTime() {
        return dataSnapshotTime;
    }

    public void setDataSnapshotTime(Date dataSnapshotTime) {
        this.dataSnapshotTime = dataSnapshotTime;
    }

    public com.latticeengines.domain.exposed.metadata.Table getDunsCount() {
        return dunsCount;
    }

    public void setDunsCount(com.latticeengines.domain.exposed.metadata.Table dunsCount) {
        this.dunsCount = dunsCount;
    }

    public Date getRefreshTime() {
        return refreshTime;
    }

    public void setRefreshTime(Date refreshTime) {
        this.refreshTime = refreshTime;
    }

    public DataReport.BasicStats getBasicStats() {
        return basicStats;
    }

    public void setBasicStats(DataReport.BasicStats basicStats) {
        this.basicStats = basicStats;
    }

    public DataReport.InputPresenceReport getInputPresenceReport() {
        return inputPresenceReport;
    }

    public void setInputPresenceReport(DataReport.InputPresenceReport inputPresenceReport) {
        this.inputPresenceReport = inputPresenceReport;
    }

    public DataReport.GeoDistributionReport getGeoDistributionReport() {
        return geoDistributionReport;
    }

    public void setGeoDistributionReport(DataReport.GeoDistributionReport geoDistributionReport) {
        this.geoDistributionReport = geoDistributionReport;
    }

    public DataReport.MatchToDUNSReport getMatchToDUNSReport() {
        return matchToDUNSReport;
    }

    public void setMatchToDUNSReport(DataReport.MatchToDUNSReport matchToDUNSReport) {
        this.matchToDUNSReport = matchToDUNSReport;
    }

    public DataReport.DuplicationReport getDuplicationReport() {
        return duplicationReport;
    }

    public void setDuplicationReport(DataReport.DuplicationReport duplicationReport) {
        this.duplicationReport = duplicationReport;
    }

    public boolean isReadyForRollup() {
        return readyForRollup;
    }

    public void setReadyForRollup(boolean readyForRollup) {
        this.readyForRollup = readyForRollup;
    }

    public RollupStatus getRollupStatus() {
        return rollupStatus;
    }

    public void setRollupStatus(RollupStatus rollupStatus) {
        this.rollupStatus = rollupStatus;
    }

    public enum Level {
        Tenant,
        Project {
            @Override
            public Level getParentLevel() {
                return Tenant;
            }
        },
        Source {
            @Override
            public Level getParentLevel() {
                return Project;
            }
        },
        Upload {
            @Override
            public Level getParentLevel() {
                return Source;
            }
        };

        public Level getParentLevel() {
            return null;
        }
    }

    /**
     * Statuses that the rollup can be in.
     */
    public enum RollupStatus {
        READY, SUBMITTED, RUNNING, FAILED_NO_RETRY
    }
}
