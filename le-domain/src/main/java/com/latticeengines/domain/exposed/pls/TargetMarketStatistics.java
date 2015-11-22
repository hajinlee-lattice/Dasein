package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.lang.builder.EqualsBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "TARGET_MARKET_STATISTICS")
public class TargetMarketStatistics implements HasPid {

    private Long pid;
    private Double expectedLift;
    private Integer numCompanies;
    private Integer numCustomers;
    private Integer numAccounts;
    private Integer marketRevenue;
    private Integer revenue;
    private Boolean isOutOfDate;

    @JsonProperty
    @Column(name = "EXPECTED_LIFT")
    public Double getExpectedLift() {
        return this.expectedLift;
    }

    @JsonProperty
    public void setExpectedLift(Double expectedLift) {
        this.expectedLift = expectedLift;
    }
    
    @JsonProperty
    @Column(name = "NUM_COMPANIES")
    public Integer getNumCompanies() {
        return this.numCompanies;
    }
    
    @JsonProperty
    public void setNumCompanies(Integer numCompanies) {
        this.numCompanies = numCompanies;
    }
    
    @JsonProperty
    @Column(name = "NUM_ACCOUNTS")
    public Integer getNumAccounts() {
        return this.numAccounts;
    }
    
    @JsonProperty
    public void setNumAccounts(Integer numAccounts) {
        this.numAccounts = numAccounts;
    }
    
    @Column(name = "NUM_CUSTOMERS")
    @JsonProperty
    public Integer getNumCustomers() {
        return this.numCustomers;
    }
    
    @JsonProperty
    public void setNumCustomers(Integer numCustomers) {
        this.numCustomers = numCustomers;
    }
    
    @Column(name = "MARKET_REVENUE")
    @JsonProperty
    public Integer getMarketRevenue() {
        return this.marketRevenue;
    }
    
    @JsonProperty
    public void setMarketRevenue(Integer marketRevenue) {
        this.marketRevenue = marketRevenue;
    }
    
    @Column(name = "REVENUE")
    @JsonProperty
    public Integer getRevenue() {
        return this.revenue;
    }
    
    @JsonProperty
    public void setRevenue(Integer revenue) {
        this.revenue = revenue;
    }
    
    @Column(name = "IS_OUT_OF_DATE")
    @JsonProperty
    public Boolean getIsOutOfDate() {
        return this.isOutOfDate;
    }
    
    @JsonProperty
    public void setIsOutOfDate(Boolean isOutOfDate) {
        this.isOutOfDate = isOutOfDate;
    }
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Override
    @Column(name = "PID", unique = true, nullable = false)
    public Long getPid() {
        return this.pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }
    
    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

}
