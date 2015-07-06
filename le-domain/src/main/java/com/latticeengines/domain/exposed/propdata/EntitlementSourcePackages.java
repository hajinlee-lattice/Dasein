package com.latticeengines.domain.exposed.propdata;

import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@Entity
@Access(AccessType.FIELD)
@Table(name = "EntitlementSourcePackages")
public class EntitlementSourcePackages implements HasPid{

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Basic(optional = false)
	@Column(name = "SourcePackage_ID", unique = true, nullable = false)
	private Long SourcePackage_ID;
	
	@Column(name = "SourcePackageName", nullable = false)
	private String SourcePackageName;
	
	@Column(name = "SourcePackageDescription", nullable = true)
	private String SourcePackageDescription;
	
	@Column(name = "Is_Default", nullable = false)
	private Boolean Is_Default;
	
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "Last_Modification_Date", nullable = false)
	private Date Last_Modification_Date;
	
	public EntitlementSourcePackages() {
		super();
	}

	@JsonProperty("SourcePackage_ID")
	public Long getSourcePackage_ID() {
		return SourcePackage_ID;
	}

	@JsonProperty("SourcePackage_ID")
	public void setSourcePackage_ID(Long sourcePackage_ID) {
		SourcePackage_ID = sourcePackage_ID;
	}

	@JsonProperty("SourcePackageName")
	public String getSourcePackageName() {
		return SourcePackageName;
	}

	@JsonProperty("SourcePackageName")
	public void setSourcePackageName(String sourcePackageName) {
		SourcePackageName = sourcePackageName;
	}

	@JsonProperty("SourcePackageDescription")
	public String getSourcePackageDescription() {
		return SourcePackageDescription;
	}

	@JsonProperty("SourcePackageDescription")
	public void setSourcePackageDescription(String sourcePackageDescription) {
		SourcePackageDescription = sourcePackageDescription;
	}

	@JsonProperty("Is_Default")
	public Boolean getIs_Default() {
		return Is_Default;
	}

	@JsonProperty("Is_Default")
	public void setIs_Default(Boolean is_Default) {
		Is_Default = is_Default;
	}

	@JsonProperty("Last_Modification_Date")
	public Date getLast_Modification_Date() {
		return Last_Modification_Date;
	}
	
	@JsonProperty("Last_Modification_Date")
	public void setLast_Modification_Date(Date last_Modification_Date) {
		Last_Modification_Date = last_Modification_Date;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((SourcePackage_ID == null) ? 0 : SourcePackage_ID.hashCode());
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
		EntitlementSourcePackages other = (EntitlementSourcePackages) obj;
		if (SourcePackage_ID == null) {
			if (other.SourcePackage_ID != null)
				return false;
		} else if (!SourcePackage_ID.equals(other.SourcePackage_ID))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "EntitlementSourcePackages [SourcePackage_ID="
				+ SourcePackage_ID + ", SourcePackageName=" + SourcePackageName
				+ ", SourcePackageDescription=" + SourcePackageDescription
				+ ", Is_Default=" + Is_Default + ", Last_Modification_Date="
				+ Last_Modification_Date + "]";
	}

	@Override
	@JsonIgnore
	public Long getPid() {
		return SourcePackage_ID;
	}

	@Override
	@JsonIgnore
	public void setPid(Long pid) {
		this.SourcePackage_ID = pid;
	}

}
