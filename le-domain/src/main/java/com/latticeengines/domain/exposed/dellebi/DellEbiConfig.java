package com.latticeengines.domain.exposed.dellebi;

import java.io.Serializable;
import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "Config")
public class DellEbiConfig implements HasPid, Serializable {

    private static final long serialVersionUID = 1L;

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
}
