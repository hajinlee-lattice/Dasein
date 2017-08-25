package com.latticeengines.domain.exposed.datacloud.customer;

import java.util.Date;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Index;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Table(name = "CUSTOMER_REPORT")
@Entity
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CustomerReport implements HasId<String>, HasPid{
    @JsonIgnore
    private Long pid;

    @JsonProperty("Id")
    private String id;

    @JsonProperty("Type")
    private CustomerReportType type;

    private ReproduceDetail reproduceDetail;

    @JsonProperty("SuggestedValue")
    private String suggestedValue;

    @JsonProperty("Comment")
    private String comment;

    @JsonProperty("ReportedByUser")
    private String reportedByUser;

    @JsonProperty("ReportedByTenant")
    private String reportedByTenant;

    @JsonProperty("CreatedTime")
    private Date createdTime;

    @JsonIgnore
    private String jiraTicket;

    @JsonProperty("MatchLog")
    private List<String> matchLog;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Column(name = "ID", unique = true, nullable = false)
    @Index(name = "Report_ID_IDX")
    @Override
    public String getId() {
        return id;
    }
    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Column(name = "TYPE")
    @Enumerated(EnumType.STRING)
    public CustomerReportType getType() {
        return type;
    }
    public void setType(CustomerReportType type) {
        this.type = type;
    }

    @Transient
    @JsonProperty("ReproduceDetail")
    public ReproduceDetail getReproduceDetail() {
        return reproduceDetail;
    }

    @Transient
    @JsonProperty("ReproduceDetail")
    public void setReproduceDetail(ReproduceDetail reproduceDetail) {
        this.reproduceDetail = reproduceDetail;
    }

    @Column(name = "REPRODUCEDETAIL")
    @Type(type = "text")
    @JsonIgnore
    private String getReproduceDetailAsString() {
        ReproduceDetail detail = getReproduceDetail();
        if (detail == null) {
            return null;
        } else {
            return JsonUtils.serialize(detail);
        }
    }

    @JsonIgnore
    private void setReproduceDetailAsString(String reproduceDetailString) {
        if (StringUtils.isEmpty(reproduceDetailString)) {
            return ;
        } else {
            setReproduceDetail(JsonUtils.deserialize(reproduceDetailString, ReproduceDetail.class));
        }
    }

    @Column(name = "SUGGESTED_VALUE")
    public String getSuggestedValue() {
        return suggestedValue;
    }

    public void setSuggestedValue(String suggestedValue) {
        this.suggestedValue = suggestedValue;
    }

    @Column(name = "COMMENT")
    @Type(type = "text")
    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Column(name = "REPORTED_BY_USER")
    public String getReportedByUser() {
        return reportedByUser;
    }

    public void setReportedByUser(String reportedByUser) {
        this.reportedByUser = reportedByUser;
    }

    @Column(name = "REPORTED_BY_TENANT")
    public String getReportedByTenant() {
        return reportedByTenant;
    }

    public void setReportedByTenant(String reportedByTenant) {
        this.reportedByTenant = reportedByTenant;
    }

    @Column(name = "CREATED_TIME")
    public Date getCreatedTime() {
        return createdTime;
    }
    public void setCreatedTime(Date createdTime) {
        this.createdTime = createdTime;
    }

    @Column(name = "JIRA_TICKET")
    public String getJiraTicket() {
        return jiraTicket;
    }

    public void setJiraTicket(String jiraTicket) {
        this.jiraTicket = jiraTicket;
    }

    @Transient
    public List<String> getMatchLog() {
        return matchLog;
    }

    @Transient
    public void setMatchLog(List<String> matchLog) {
        this.matchLog = matchLog;
    }

}
