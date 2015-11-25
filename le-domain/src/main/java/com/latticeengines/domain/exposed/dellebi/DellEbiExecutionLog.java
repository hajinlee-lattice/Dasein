package com.latticeengines.domain.exposed.dellebi;

import java.io.Serializable;
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

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "Execution_Log")
public class DellEbiExecutionLog implements HasPid, Serializable {

	private static final long serialVersionUID = -6786294520901179716L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Basic(optional = false)
	@Column(name = "Id", unique = true, nullable = false)
	private Long id;

	@Column(name = "FileName", nullable = false)
	private String fileName;

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "StartDate", nullable = true)
	private Date startDate;

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "EndDate", nullable = true)
	private Date endDate;

	@Column(name = "Status", nullable = false)
	private int status;

	@Column(name = "Error", nullable = true)
	private String error;

	@Override
	public Long getPid() {
		return id;
	}

	@Override
	public void setPid(Long pid) {
		this.id = pid;
	}

	public String getFile() {
		return this.fileName;
	}

	public void setFile(String aFileName) {
		this.fileName = aFileName;
	}

	public Date getStartDate() {
		return this.startDate;
	}

	public void setStartDate(Date aStartDate) {
		this.startDate = aStartDate;
	}

	public Date getEndDate() {
		return this.endDate;
	}

	public void setEndDate(Date aEndDate) {
		this.endDate = aEndDate;
	}

	public int getStatus() {
		return this.status;
	}

	public void setStatus(int aStatus) {
		this.status = aStatus;
	}

	public String getError() {
		return this.error;
	}

	public void setError(String aError) {
		this.error = aError;
	}

}
