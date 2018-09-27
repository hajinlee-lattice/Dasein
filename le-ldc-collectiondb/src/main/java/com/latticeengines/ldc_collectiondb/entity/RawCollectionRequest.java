package com.latticeengines.ldc_collectiondb.entity;

import java.sql.Timestamp;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "RawCollectionRequest")
public class RawCollectionRequest {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private long pid;

    @Column(name = "VENDOR", nullable = false)
    private String vendor;

    @Column(name = "DOMAIN", nullable = false)
    private String domain;

    @Column(name = "ORIGINAL_REQUEST_ID", nullable = false)
    private String originalRequestId;

    @Column(name = "REQUESTED_TIME", nullable = false)
    private Timestamp requestedTime;

    @Column(name = "TRANSFERRED", nullable = false)
    private boolean transferred;

    public long getPid() {

        return pid;

    }

    public void setPid(long pid) {

        this.pid = pid;

    }

    public String getVendor() {

        return vendor;

    }

    public void setVendor(String vendor) {

        this.vendor = vendor.toUpperCase();

    }

    public String getDomain() {

        return domain;

    }

    public void setDomain(String domain) {

        this.domain = domain;

    }

    public String getOriginalRequestId() {

        return originalRequestId;

    }

    public void setOriginalRequestId(String originalRequestId) {

        this.originalRequestId = originalRequestId;

    }

    public Timestamp getRequestedTime() {

        return requestedTime;

    }

    public void setRequestedTime(Timestamp requestedTime) {

        this.requestedTime = requestedTime;

    }

    public boolean getTransferred() {

        return transferred;

    }

    public void setTransferred(boolean transferred) {

        this.transferred = transferred;

    }
}
