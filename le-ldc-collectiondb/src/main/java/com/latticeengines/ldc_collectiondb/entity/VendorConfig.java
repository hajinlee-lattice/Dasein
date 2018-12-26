package com.latticeengines.ldc_collectiondb.entity;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

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
    public static final Set<String> EFFECTIVE_VENDOR_SET = new HashSet<>();

    static {
        EFFECTIVE_VENDOR_SET.add(VENDOR_BUILTWITH);
        EFFECTIVE_VENDOR_SET.add(VENDOR_ALEXA);
        EFFECTIVE_VENDOR_SET.add(VENDOR_SEMRUSH);
        EFFECTIVE_VENDOR_SET.add(VENDOR_ORBI_V2);
    }


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
    private long collectingFreq;//collection period in seconds

    @Column(name = "MAX_ACTIVE_TASKS", nullable = false)
    private int maxActiveTasks;

    @Column(name = "GROUP_BY", nullable = false)
    private String groupBy;

    @Column(name = "SORT_BY", nullable = false)
    private String sortBy;

    @Column(name = "CONSOLIDATION_PERIOD", nullable = false)
    private long consolidationPeriod;//in seconds

    @Column(name = "LAST_CONSOLIDATED")
    private Timestamp lastConsolidated;

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

    public String getGroupBy() {
        return groupBy;
    }

    public String getSortBy() {
        return sortBy;
    }

    public void setGroupBy(String groupBy) {
        this.groupBy = groupBy;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    public long getConsolidationPeriod() {
        return consolidationPeriod;
    }

    public Timestamp getLastConsolidated() {
        return lastConsolidated;
    }

    public void setConsolidationPeriod(long consolidationPeriod) {
        this.consolidationPeriod = consolidationPeriod;
    }

    public void setLastConsolidated(Timestamp lastConsolidated) {
        this.lastConsolidated = lastConsolidated;
    }
}
