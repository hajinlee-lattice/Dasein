package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.dataplatform.HasId;


@Entity
@Table(name = "AccountMaster_Accounts")
public class Company implements HasPid, Serializable {

    private Long pid;
    private String name;
    private String domain;
    private String street;
    private String city;
    private String state;
    private String region;
    private String country;
    private String industry; 
    private String subIndustry;
    private String revenueRange;
    private String employeesRange;
    private Long parentId;
    private Integer insideViewId;

    @Id
//  @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Override
    @JsonIgnore
    @Column(name = "LatticeAccountID", unique = true, nullable = false)
    public Long getPid() {
        return this.pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("LatticeAccountID")
    @Transient
    public Long getAccountId() {
        return this.pid;
    }

    @JsonProperty("LatticeAccountID")
    public void setAccountId(Long accountId) {
        this.pid = accountId;
    }

    @JsonProperty("Domain")
    @Column(name = "Domain", nullable = true)
    public String getDomain() {
        return domain;
    }

    @JsonProperty("Domain")
    public void setDomain(String domain) {
        this.domain = domain;
    }

    @JsonProperty("Name")
    @Column(name = "Name", nullable = true)
    public String getName() {
        return name;
    }

    @JsonProperty("Name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("Street")
    @Column(name = "Street", nullable = true)
    public String getStreet() {
        return street;
    }

    @JsonProperty("Street")
    public void setStreet(String street) {
        this.street = street;
    }

    @JsonProperty("City")
    @Column(name = "City", nullable = true)
    public String getCity() {
        return city;
    }

    @JsonProperty("City")
    public void setCity(String city) {
        this.city = city;
    }

    @JsonProperty("State")
    @Column(name = "State", nullable = true)
    public String getState() {
        return state;
    }

    @JsonProperty("State")
    public void setState(String state) {
        this.state = state;
    }

    @JsonProperty("Region")
    @Column(name = "Region", nullable = true)
    public String getRegion() {
        return region;
    }

    @JsonProperty("Region")
    public void setRegion(String region) {
        this.region = region;
    }

    @JsonProperty("Country")
    @Column(name = "Country", nullable = true)
    public String getCountry() {
        return country;
    }

    @JsonProperty("Country")
    public void setCountry(String country) {
        this.country = country;
    }

    @JsonProperty("Industry")
    @Column(name = "Industry", nullable = true)
    public String getIndustry() {
        return industry;
    }

    @JsonProperty("Industry")
    public void setIndustry(String industry) {
        this.industry = industry;
    }

    @JsonProperty("SubIndustry")
    @Column(name = "SubIndustry", nullable = true)
    public String getSubIndustry() {
        return subIndustry;
    }

    @JsonProperty("SubIndustry")
    public void setSubIndustry(String subIndustry) {
        this.subIndustry = subIndustry;
    }

    @JsonProperty("RevenueRange")
    @Column(name = "RevenueRange", nullable = true)
    public String getRevenueRange() {
        return revenueRange;
    }

    @JsonProperty("RevenueRange")
    public void setRevenueRange(String revenueRange) {
        this.revenueRange = revenueRange;
    }

    @JsonProperty("EmployeesRange")
    @Column(name = "EmployeesRange", nullable = true)
    public String getEmployeesRange() {
        return employeesRange;
    }

    @JsonProperty("EmployeesRange")
    public void setEmployeesRange(String employeesRange) {
        this.employeesRange = employeesRange;
    }

    @JsonProperty("ParentID")
    @Column(name = "ParentID", nullable = true)
    public Long getParentId() {
        return parentId;
    }

    @JsonProperty("ParentID")
    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    @JsonProperty("InsideViewID")
    @Column(name = "InsideViewID", nullable = true)
    public Integer getInsideViewId() {
        return insideViewId;
    }

    @JsonProperty("InsideViewID")
    public void setInsideViewId(Integer insideViewId) {
        this.insideViewId = insideViewId;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
