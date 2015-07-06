package com.latticeengines.domain.exposed.propdata;

import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
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
@Table(name = "EntitlementSourcePackageMap")
public class EntitlementSourcePackageMap implements HasPid{

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Basic(optional = false)
	@Column(name = "EntitlementSourcePackageMap_ID", unique = true, nullable = false)
	private Long EntitlementSourcePackageMap_ID;
	
	@Column(name = "SourcePackage_ID", nullable = false, insertable = false, updatable = false)
	private Long SourcePackage_ID;
	
	@Column(name = "IsDomainBased", nullable = false)
	private Boolean IsDomainBased;
	
	@Column(name = "Lookup_ID", nullable = false)
	private String Lookup_ID;
	
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "Last_Modification_Date", nullable = false)
	private Date Last_Modification_Date;
	
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="SourcePackage_ID")
	private EntitlementSourcePackages entitlementSourcePackage;
	
	public EntitlementSourcePackageMap() {
		super();
	}

	@JsonProperty("EntitlementSourcePackageMap_ID")
	public Long getEntitlementSourcePackageMap_ID() {
		return EntitlementSourcePackageMap_ID;
	}

	@JsonProperty("EntitlementSourcePackageMap_ID")
	public void setEntitlementSourcePackageMap_ID(
			Long entitlementSourcePackageMap_ID) {
		EntitlementSourcePackageMap_ID = entitlementSourcePackageMap_ID;
	}

	@JsonProperty("SourcePackage_ID")
	public Long getSourcePackage_ID() {
		return SourcePackage_ID;
	}

	@JsonProperty("SourcePackage_ID")
	public void setSourcePackage_ID(Long sourcePackage_ID) {
		SourcePackage_ID = sourcePackage_ID;
	}

	@JsonProperty("IsDomainBased")
	public Boolean getIsDomainBased() {
		return IsDomainBased;
	}

	@JsonProperty("IsDomainBased")
	public void setIsDomainBased(Boolean isDomainBased) {
		IsDomainBased = isDomainBased;
	}

	@JsonProperty("Lookup_ID")
	public String getLookup_ID() {
		return Lookup_ID;
	}

	@JsonProperty("Lookup_ID")
	public void setLookup_ID(String lookup_ID) {
		Lookup_ID = lookup_ID;
	}

	@JsonProperty("Last_Modification_Date")
	public Date getLast_Modification_Date() {
		return Last_Modification_Date;
	}

	@JsonProperty("Last_Modification_Date")
	public void setLast_Modification_Date(Date last_Modification_Date) {
		Last_Modification_Date = last_Modification_Date;
	}

	@JsonIgnore
	public EntitlementSourcePackages getEntitlementSourcePackage() {
		return entitlementSourcePackage;
	}

	@JsonIgnore
	public void setEntitlementSourcePackage(
			EntitlementSourcePackages entitlementSourcePackage) {
		this.entitlementSourcePackage = entitlementSourcePackage;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((EntitlementSourcePackageMap_ID == null) ? 0
						: EntitlementSourcePackageMap_ID.hashCode());
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
		EntitlementSourcePackageMap other = (EntitlementSourcePackageMap) obj;
		if (EntitlementSourcePackageMap_ID == null) {
			if (other.EntitlementSourcePackageMap_ID != null)
				return false;
		} else if (!EntitlementSourcePackageMap_ID
				.equals(other.EntitlementSourcePackageMap_ID))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "EntitlementSourcePackageMap [EntitlementSourcePackageMap_ID="
				+ EntitlementSourcePackageMap_ID + ", SourcePackage_ID="
				+ SourcePackage_ID + ", IsDomainBased=" + IsDomainBased
				+ ", Lookup_ID=" + Lookup_ID + ", Last_Modification_Date="
				+ Last_Modification_Date + ", entitlementSourcePackage="
				+ entitlementSourcePackage + "]";
	}

	@Override
	@JsonIgnore
	public Long getPid() {
		return EntitlementSourcePackageMap_ID;
	}

	@Override
	@JsonIgnore
	public void setPid(Long pid) {
		this.EntitlementSourcePackageMap_ID = pid;
	}

}
