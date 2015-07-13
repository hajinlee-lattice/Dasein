package com.latticeengines.domain.exposed.propdata;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Access(AccessType.FIELD)
@Table(name = "DataColumnMap")
public class DataColumnMap implements HasPid {

	@Id
	@Column(name = "ColumnCalcID", unique = true, nullable = false)
	private Long ColumnCalcID;
	
	@Column(name = "Extension_Name", nullable = false)
	private String Extension_Name;

	@Column(name = "Value_Type_String", nullable = false)
	private String Value_Type_String;
	
	@Column(name = "Value_Length", nullable = true)
	private Integer Value_Length;
	
	@Column(name = "Depth", nullable = true)
	private Integer Depth;
	
	@Column(name = "Offset", nullable = true)
	private Integer Offset;
	
	@Column(name = "DataProviderName", nullable = false)
	private String DataProviderName;
	
	@Column(name = "SourceTableName", nullable = false)
	private String SourceTableName;
	
	@Column(name = "SourceColumnName", nullable = false)
	private String SourceColumnName;
	
	@Column(name = "IsDomainFeatureTable", nullable = false)
	private Boolean IsDomainFeatureTable;
	
	@Column(name = "IsSupported", nullable = false)
	private Boolean IsSupported;
	
	@Column(name = "RowNum", nullable = true)
	private Integer RowNum;
	
	public DataColumnMap() {
		super();
	}

	@JsonProperty("ColumnCalcID")
	public Long getColumnCalcID() {
		return ColumnCalcID;
	}

	@JsonProperty("ColumnCalcID")
	public void setColumnCalcID(Long columnCalcID) {
		ColumnCalcID = columnCalcID;
	}

	@JsonProperty("Extension_Name")
	public String getExtension_Name() {
		return Extension_Name;
	}

	@JsonProperty("Extension_Name")
	public void setExtension_Name(String extension_Name) {
		Extension_Name = extension_Name;
	}

	@JsonProperty("Value_Type_String")
	public String getValue_Type_String() {
		return Value_Type_String;
	}

	@JsonProperty("Value_Type_String")
	public void setValue_Type_String(String value_Type_String) {
		Value_Type_String = value_Type_String;
	}

	@JsonProperty("Value_Length")
	public Integer getValue_Length() {
		return Value_Length;
	}

	@JsonProperty("Value_Length")
	public void setValue_Length(Integer value_Length) {
		Value_Length = value_Length;
	}

	@JsonProperty("Depth")
	public Integer getDepth() {
		return Depth;
	}

	@JsonProperty("Depth")
	public void setDepth(Integer depth) {
		Depth = depth;
	}

	@JsonProperty("Offset")
	public Integer getOffset() {
		return Offset;
	}

	@JsonProperty("Offset")
	public void setOffset(Integer offset) {
		Offset = offset;
	}

	@JsonProperty("DataProviderName")
	public String getDataProviderName() {
		return DataProviderName;
	}

	@JsonProperty("DataProviderName")
	public void setDataProviderName(String dataProviderName) {
		DataProviderName = dataProviderName;
	}

	@JsonProperty("SourceTableName")
	public String getSourceTableName() {
		return SourceTableName;
	}

	@JsonProperty("SourceTableName")
	public void setSourceTableName(String sourceTableName) {
		SourceTableName = sourceTableName;
	}

	@JsonProperty("SourceColumnName")
	public String getSourceColumnName() {
		return SourceColumnName;
	}

	@JsonProperty("SourceColumnName")
	public void setSourceColumnName(String sourceColumnName) {
		SourceColumnName = sourceColumnName;
	}

	@JsonProperty("IsDomainFeatureTable")
	public Boolean getIsDomainFeatureTable() {
		return IsDomainFeatureTable;
	}

	@JsonProperty("IsDomainFeatureTable")
	public void setIsDomainFeatureTable(Boolean isDomainFeatureTable) {
		IsDomainFeatureTable = isDomainFeatureTable;
	}

	@JsonProperty("IsSupported")
	public Boolean getIsSupported() {
		return IsSupported;
	}

	@JsonProperty("IsSupported")
	public void setIsSupported(Boolean isSupported) {
		IsSupported = isSupported;
	}

	@JsonProperty("RowNum")
	public Integer getRowNum() {
		return RowNum;
	}

	@JsonProperty("RowNum")
	public void setRowNum(Integer rowNum) {
		RowNum = rowNum;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((ColumnCalcID == null) ? 0 : ColumnCalcID.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataColumnMap other = (DataColumnMap) obj;
		if (ColumnCalcID == null) {
			if (other.ColumnCalcID != null)
				return false;
		} else if (!ColumnCalcID.equals(other.ColumnCalcID))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "DataColumnMap [ColumnCalcID=" + ColumnCalcID
				+ ", Extension_Name=" + Extension_Name + ", Value_Type_String="
				+ Value_Type_String + ", Value_Length=" + Value_Length
				+ ", Depth=" + Depth + ", Offset=" + Offset
				+ ", DataProviderName=" + DataProviderName
				+ ", SourceTableName=" + SourceTableName
				+ ", SourceColumnName=" + SourceColumnName
				+ ", IsDomainFeatureTable=" + IsDomainFeatureTable
				+ ", IsSupported=" + IsSupported + ", RowNum=" + RowNum + "]";
	}

	@Override
	@JsonIgnore
	public Long getPid() {
		return ColumnCalcID;
	}

	@Override
	@JsonIgnore
	public void setPid(Long pid) {
		this.ColumnCalcID = pid;
	}

}
