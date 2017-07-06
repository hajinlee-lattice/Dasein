package com.latticeengines.domain.exposed.datacloud;

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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MadisonLogicDailyProgress")
public class MadisonLogicDailyProgress implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "ID", unique = true, nullable = false)
    private Long id;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "Date", nullable = false)
    private Date createTime;

    @Column(name = "Status", nullable = false)
    private String status;

    @Column(name = "CurrentBlock", nullable = true)
    private Long currentBlock;

    @Column(name = "RawSize", nullable = true)
    private Long rawSize;

    @Column(name = "DestinationTable", nullable = false)
    private String destinationTable;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "FileDate", nullable = true)
    private Date fileDate;

    @Column(name = "ErrorMessage", nullable = true)
    private String errorMessage;

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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getCurrentBlock() {
        return currentBlock;
    }

    public void setCurrentBlock(Long currentBlock) {
        this.currentBlock = currentBlock;
    }

    public Long getRawSize() {
        return rawSize;
    }

    public void setRawSize(Long rawSize) {
        this.rawSize = rawSize;
    }

    public String getDestinationTable() {
        return destinationTable;
    }

    public void setDestinationTable(String destinationTable) {
        this.destinationTable = destinationTable;
    }

    public Date getFileDate() {
        return fileDate;
    }

    public void setFileDate(Date fileDate) {
        this.fileDate = fileDate;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
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

        MadisonLogicDailyProgress theOther = (MadisonLogicDailyProgress) other;

        return new EqualsBuilder().append(id, theOther.getPid()).isEquals();

    }

    @Override
    public String toString() {
        return "MadisonLogicDailyProgress="
                + new ToStringBuilder(id).append(id).append(createTime).append(status).append(destinationTable)
                        .append(fileDate).append(errorMessage).toString();
    }

}
