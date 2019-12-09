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
@Table
public class CollectionRequest {

    public static final String STATUS_READY = "READY";
    public static final String STATUS_COLLECTING = "COLLECTING";
    public static final String STATUS_DELIVERED = "DELIVERED";
    public static final String STATUS_FAILED = "FAILED";
    public static final String STATUS_EMPTY_RESULT = "EMPTY_RESULT";

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private long pid;

    @Column(name = "DOMAIN", nullable = false)
    private String domain;

    @Column(name = "ORIGINAL_REQUEST_ID", nullable = false)
    private String originalRequestId;

    @Column(name = "REQUESTED_TIME", nullable = false)
    private Timestamp requestedTime;

    @Column(name = "VENDOR", nullable = false)
    private String vendor;

    @Column(name = "PICKUP_TIME")
    private Timestamp pickupTime;

    @Column(name = "PICKUP_WORKER")
    private String pickupWorker;

    @Column(name = "STATUS", nullable = false)
    private String status;

    @Column(name = "DELIVERY_TIME")
    private Timestamp deliveryTime;

    @Column(name = "RETRY_ATTEMPTS", nullable = false)
    private int retryAttempts;

    public long getPid() {

        return pid;

    }

    public void setPid(long pid) {

        this.pid = pid;

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

    public String getVendor() {

        return vendor;

    }

    public void setVendor(String vendor) {

        this.vendor = vendor;

    }

    public Timestamp getPickupTime() {

        return pickupTime;

    }

    public void setPickupTime(Timestamp pickupTime) {

        this.pickupTime = pickupTime;

    }

    public String getPickupWorker() {

        return pickupWorker;

    }

    public void setPickupWorker(String pickupWorker) {

        this.pickupWorker = pickupWorker;

    }

    public String getStatus() {

        return status;

    }

    public void setStatus(String status) {

        this.status = status;

    }

    public Timestamp getDeliveryTime() {

        return deliveryTime;

    }

    public void setDeliveryTime(Timestamp deliveryTime) {

        this.deliveryTime = deliveryTime;

    }

    public int getRetryAttempts() {

        return retryAttempts;

    }

    public void setRetryAttempts(int retryAttempts) {

        this.retryAttempts = retryAttempts;

    }
}
