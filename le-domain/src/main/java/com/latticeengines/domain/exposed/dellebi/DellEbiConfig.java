package com.latticeengines.domain.exposed.dellebi;

import java.io.Serializable;
import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "Config")
public class DellEbiConfig implements HasPid, Serializable, Comparable<DellEbiConfig> {

    protected static final Logger log = LoggerFactory.getLogger(DellEbiConfig.class);
    private static final long serialVersionUID = 3568465584606850312L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "Id", unique = true, nullable = false)
    private Long id;

    @Column(name = "Type", nullable = false)
    private String type;

    @Column(name = "InputFields", nullable = false)
    private String inputFields;

    @Column(name = "Headers", nullable = false)
    private String headers;

    @Column(name = "OutputFields", nullable = false)
    private String outputFields;

    @Column(name = "TargetColumns", nullable = false)
    private String targetColumns;

    @Column(name = "TargetTable", nullable = false)
    private String targetTable;

    @Column(name = "StartDate", nullable = true)
    private Date startDate;

    @Column(name = "IsDeleted", nullable = true)
    private Boolean isDeleted;

    @Column(name = "Bean", nullable = false)
    private String bean;

    @Column(name = "IsActive", nullable = false)
    private Boolean isActive;

    @Column(name = "InboxPath", nullable = false)
    private String inboxPath;

    @Column(name = "Priority", nullable = false)
    private int priority;

    @Column(name = "QuartzJob", nullable = false)
    private String quartzJob;

    @Column(name = "FilePattern", nullable = false)
    private String filePattern;

    @Column(name = "PostStoreProcedure", nullable = true)
    private String postStoreProcedure;

    @Override
    public Long getPid() {
        return id;
    }

    @Override
    public void setPid(Long pid) {
    }

    public String getTargetColumns() {
        return targetColumns;
    }

    public String getInputFields() {
        return inputFields;
    }

    public String getOutputFields() {
        return outputFields;
    }

    public String getHeaders() {
        return headers;
    }

    public String getType() {
        return type;
    }

    public Date getStartDate() {
        return startDate;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public Boolean getIsDeleted() {
        return isDeleted;
    }

    public String getBean() {
        return bean;
    }

    public Boolean getIsActive() {
        return isActive;
    }

    public String getInboxPath() {
        return inboxPath;
    }

    public int getPriority() {
        return priority;
    }

    public String getFilePattern() {
        return filePattern;
    }

    public String getQuartzJob() {
        return quartzJob;
    }

    public String getPostStoreProcedure() {
        return postStoreProcedure;
    }

    @Override
    public int compareTo(DellEbiConfig dellEbiConfig) {
        // Asc
        return ObjectUtils.compare(this.priority,
                dellEbiConfig != null ? dellEbiConfig.getPriority() : null);
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringBuilder.getDefaultStyle())
                .toString();
    }

}
