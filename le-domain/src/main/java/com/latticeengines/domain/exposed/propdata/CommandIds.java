package com.latticeengines.domain.exposed.propdata;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.apache.commons.lang.builder.EqualsBuilder;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "CommandIds")
public class CommandIds implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "CommandId", unique = true, nullable = false)
    private Long id;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "CreateTime", nullable = false)
    private Date createTime;
    
    @Column(name = "CreatedBy", nullable = false)
    private String createdBy;

    @Override
    public Long getPid() {
        return this.id;
    }

    @Override
    public void setPid(Long id) {
        this.id = id;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    @Override
    public int hashCode() {
        int result;
        result = getPid().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object other) {

        if (other == null) {
            return false;
        }
        if (other == this) {
            return true;
        }
        if (!other.getClass().equals(this.getClass())) {
            return false;
        }

        CommandIds theOther = (CommandIds) other;

        return new EqualsBuilder().append(id, theOther.getPid()).isEquals();

    }

    @Override
    public String toString() {
        return "CommandId [id=" + id + ", createTime=" + createTime
                + ", createdBy=" + createdBy + "]";
    }
    
    
}
