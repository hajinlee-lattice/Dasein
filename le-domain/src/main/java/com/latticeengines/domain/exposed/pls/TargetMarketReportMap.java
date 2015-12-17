package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "TARGET_MARKET_REPORT_MAP")
public class TargetMarketReportMap implements HasPid {
    private Long pid;
    private String reportName;
    private Report report;
    private TargetMarket targetMarket;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Override
    @Column(name = "PID", unique = true, nullable = false)
    public Long getPid() {
        return pid;
    }

    @JsonIgnore
    @JoinColumn(name = "TARGET_MARKET_ID", nullable = false)
    @OneToOne(cascade = CascadeType.MERGE, fetch = FetchType.LAZY)
    @OnDelete(action = OnDeleteAction.CASCADE)
    public TargetMarket getTargetMarket() {
        return this.targetMarket;
    }

    @JsonIgnore
    public void setTargetMarket(TargetMarket targetMarket) {
        this.targetMarket = targetMarket;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonIgnore
    @JoinColumn(name = "REPORT_ID", nullable = false)
    @OneToOne(cascade = CascadeType.REMOVE)
    public Report getReport() {
        return this.report;
    }

    @JsonIgnore
    public void setReport(Report report) {
        this.report = report;
        if (report != null) {
            this.reportName = report.getName();
        }
    }

    @JsonProperty("report_name")
    @Transient
    public String getReportName() {
        return reportName;
    }

    @JsonProperty("report_name")
    @Transient
    public void setReportName(String reportName) {
        this.reportName = reportName;
    }

}
