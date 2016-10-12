package com.latticeengines.domain.exposed.datacloud.manage;


import java.io.Serializable;
import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "DataCloudVersion")
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataCloudVersion implements HasPid, Serializable {

    private static final long serialVersionUID = -6472245305360293747L;

	@Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Index(name = "IX_VERSION")
    @Column(name = "Version", nullable = false, unique = true, length = 10)
    private String version;

    @Index(name = "IX_MAJOR")
    @Column(name = "MajorVersion", nullable = false, length = 10)
    private String majorVersion;

    @Column(name = "CreateDate", nullable = false)
    private Date createDate;

    @Column(name = "AccountMasterHdfsVersion", nullable = false)
    private String accountMasterHdfsVersion;

    @Column(name = "AccountLookupHdfsVersion", nullable = false)
    private String accountLookupHdfsVersion;

    @Column(name = "DynamoTableSignature", length = 100)
    private String dynamoTableSignature;

    @Enumerated(EnumType.STRING)
    @Column(name = "Status", nullable = false)
    private Status status;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getAccountMasterHdfsVersion() {
        return accountMasterHdfsVersion;
    }

    public void setAccountMasterHdfsVersion(String accountMasterHdfsVersion) {
        this.accountMasterHdfsVersion = accountMasterHdfsVersion;
    }

    public String getAccountLookupHdfsVersion() {
        return accountLookupHdfsVersion;
    }

    public void setAccountLookupHdfsVersion(String accountLookupHdfsVersion) {
        this.accountLookupHdfsVersion = accountLookupHdfsVersion;
    }

    public String getMajorVersion() {
        return majorVersion;
    }

    public void setMajorVersion(String majorVersion) {
        this.majorVersion = majorVersion;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getDynamoTableSignature() {
        return dynamoTableSignature;
    }

    public void setDynamoTableSignature(String dynamoTableSignature) {
        this.dynamoTableSignature = dynamoTableSignature;
    }

    public enum Status {
        NEW, APPROVED, DEPRECATED
    }
}
