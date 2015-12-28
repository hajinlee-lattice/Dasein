package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.dataplatform.HasId;

@Entity
@Table(name = "AccountMaster_Contacts")
public class Contact implements HasId<String>, HasPid, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -5828315692148383419L;
    private Long pid;
    private String id;

    private Long companyId;
    private String firstName;
    private String lastName;
    private String titles;
    private String email; 
    private String phone; 
    private String jobLevel; 
    private String jobType; 

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Override
    @JsonIgnore
    @Column(name = "PID", unique = true, nullable = false)
    public Long getPid() {
        return this.pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty
    @Id
    @Column(name = "ContactID", unique = true, nullable = false)
    public String getId() {
        return this.id;
    }

    @JsonProperty
    public void setId(String id) {
        this.id = id;
    }


    @JsonProperty("FirstName")
    @Column(name = "FirstName", nullable = true)
    public String getFirstName() {
        return firstName;
    }

    @JsonProperty("FirstName")
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    @JsonProperty("LastName")
    @Column(name = "LastName", nullable = true)
    public String getLastName() {
        return lastName;
    }

    @JsonProperty("LastName")
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @JsonProperty("Email")
    @Column(name = "Email", nullable = true)
    public String getEmail() {
        return email;
    }

    @JsonProperty("Email")
    public void setEmail(String email) {
        this.email = email;
    }

    @JsonProperty("Titles")
    @Column(name = "Titles", nullable = true)
    public String getTitles() {
        return titles; 
    }

    @JsonProperty("Titles")
    public void setTitles(String titles) {
        this.titles = titles;
    }

    @JsonProperty("Phone")
    @Column(name = "Phone", nullable = true)
    public String getPhone() {
        return phone; 
    }

    @JsonProperty("Phone")
    public void setPhone(String phone) {
        this.phone = phone;
    }

    @JsonProperty("JobLevel")
    @Column(name = "JobLevel", nullable = true)
    public String getJobLevel() {
        return jobLevel; 
    }

    @JsonProperty("JobLevel")
    public void setJobLevel(String jobLevel) {
        this.jobLevel = jobLevel;
    }

    @JsonProperty("JobType")
    @Column(name = "JobType", nullable = true)
    public String getJobType() {
        return jobType; 
    }

    @JsonProperty("JobType")
    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    @JsonProperty("CompanyID")
    @Column(name = "CompanyID", nullable = true)
    public Long getCompanyId() {
        return companyId; 
    }

    @JsonProperty("CompanyID")
    public void setCompanyId(Long companyId) {
        this.companyId = companyId;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
