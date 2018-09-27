package com.latticeengines.ldc_collectiondb.entity;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "VendorConfig")
public class VendorConfig {

    public static final String VENDOR_ALEXA = "ALEXA";
    public static final String VENDOR_BUILTWITH = "BUILTWITH";
    public static final String VENDOR_COMPETE = "COMPETE";
    public static final String VENDOR_FEATURE = "FEATURE";
    public static final String VENDOR_HPA_NEW = "HPA_NEW";
    public static final String VENDOR_ORBI_V2 = "ORBINTELLIGENCEV2";
    public static final String VENDOR_SEMRUSH = "SEMRUSH";

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private long pid;

    @Column(name = "VENDOR", nullable = false)
    private String vendor;

    @Column(name = "DOMAIN_FIELD", nullable = false)
    private String domainField;

    @Column(name = "DOMAIN_CHECK_FIELD", nullable = false)
    private String domainCheckField;

    @Column(name = "COLLECTING_FREQ", nullable = false)
    private long collectingFreq;

    @Column(name = "MAX_ACTIVE_TASKS", nullable = false)
    private int maxActiveTasks;

    public long getPid() {

        return pid;

    }

    public String getVendor() {

        return vendor;

    }

    public String getDomainField() {

        return domainField;

    }

    public String getDomainCheckField() {

        return domainCheckField;

    }

    public long getCollectingFreq() {

        return collectingFreq;

    }

    public int getMaxActiveTasks() {

        return maxActiveTasks;

    }

    public void setPid(long pid) {

        this.pid = pid;

    }

    public void setVendor(String vendor) {

        this.vendor = vendor;

    }

    public void setDomainField(String domainField) {

        this.domainField = domainField;

    }

    public void setDomainCheckField(String domainCheckField) {

        this.domainCheckField = domainCheckField;

    }

    public void setCollectingFreq(long collectingFreq) {

        this.collectingFreq = collectingFreq;

    }

    public void setMaxActiveTasks(int maxActiveTasks) {

        this.maxActiveTasks = maxActiveTasks;

    }
}
