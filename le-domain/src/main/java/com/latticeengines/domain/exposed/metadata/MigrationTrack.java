package com.latticeengines.domain.exposed.metadata;

import java.util.Map;

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
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "MIGRATION_TRACK")
@JsonIgnoreProperties(ignoreUnknown = true)
public class MigrationTrack implements HasPid, HasTenant {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_COLLECTION_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private DataCollection dataCollection;

    @OneToOne(cascade = CascadeType.REMOVE)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @Column(name = "STATUS", nullable = false)
    @JsonProperty("status")
    @Enumerated(EnumType.STRING)
    private Status status;

    @JsonProperty("version")
    @Enumerated(EnumType.STRING)
    @Column(name = "VERSION", nullable = false)
    private DataCollection.Version version;

    @JsonProperty("curActiveTable")
    @Type(type = "json")
    @Column(name = "CUR_ACTIVE_TABLE_NAME", columnDefinition = "'JSON'")
    // TODO - Role -> Table.PID
    private Map<TableRoleInCollection, Long> curActiveTable; // List of all active tables' names under tenant

    @Type(type = "json")
    @JsonProperty("importAction")
    @Column(name = "IMPORT_ACTION", columnDefinition = "'JSON'")
    private MigrationTrackImportAction importAction; // mimic import actions

    @Column(name = "CUBES_DATA")
    @Lob
    @JsonIgnore
    private byte[] statsCubesData;

    @Column(name = "DATA")
    @Lob
    @JsonIgnore
    private byte[] statsData;

    @Column(name = "NAME", nullable = false)
    @JsonProperty("statsName")
    private String statsName;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public DataCollection getDataCollection() {
        return dataCollection;
    }

    public void setDataCollection(DataCollection dataCollection) {
        this.dataCollection = dataCollection;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public DataCollection.Version getVersion() {
        return version;
    }

    public void setVersion(DataCollection.Version version) {
        this.version = version;
    }

    public Map<TableRoleInCollection, Long> getCurActiveTable() {
        return curActiveTable;
    }

    public void setCurActiveTable(Map<TableRoleInCollection, Long> curActiveTable) {
        this.curActiveTable = curActiveTable;
    }

    public MigrationTrackImportAction getImportAction() {
        return importAction;
    }

    public void setImportAction(MigrationTrackImportAction importAction) {
        this.importAction = importAction;
    }

    public byte[] getStatsCubesData() {
        return statsCubesData;
    }

    public void setStatsCubesData(byte[] statsCubesData) {
        this.statsCubesData = statsCubesData;
    }

    public byte[] getStatsData() {
        return statsData;
    }

    public void setStatsData(byte[] statsData) {
        this.statsData = statsData;
    }

    public String getStatsName() {
        return statsName;
    }

    public void setStatsName(String statsName) {
        this.statsName = statsName;
    }

    public enum Status {
        SCHEDULED, STARTED, FAILED, COMPLETED;

        public Status complement() {
            switch (this) {
                case SCHEDULED:
                    return SCHEDULED;
                case STARTED:
                    return STARTED;
                case FAILED:
                    return FAILED;
                case COMPLETED:
                default:
                    return COMPLETED;
            }
        }

    }
}
