package com.latticeengines.domain.exposed.datacloud.manage;



import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Access(AccessType.FIELD)
@Table(name = "DataBlockDomainEntitlement")
public class DataBlockDomainEntitlement {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Enumerated(EnumType.STRING)
    @Column(name = "Domain", nullable = false, length = 100)
    private DataDomain domain;

    @Enumerated(EnumType.STRING)
    @Column(name = "RecordType", nullable = false, length = 20)
    private DataRecordType recordType;

    @ManyToOne
    @JoinColumn(name = "DataBlockLevel", nullable = false)
    private DataBlockLevelMetadata dataBlockLevel;

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public DataDomain getDomain() {
        return domain;
    }

    public void setDomain(DataDomain domain) {
        this.domain = domain;
    }

    public DataRecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(DataRecordType recordType) {
        this.recordType = recordType;
    }

    public DataBlockLevelMetadata getDataBlockLevel() {
        return dataBlockLevel;
    }

    public void setDataBlockLevel(DataBlockLevelMetadata dataBlockLevel) {
        this.dataBlockLevel = dataBlockLevel;
    }
}
