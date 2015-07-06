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
@Table(name = "EntitlementSourceColumnsContractPackageMap")
public class EntitlementSourceColumnsContractPackageMap implements HasPid{

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Basic(optional = false)
	@Column(name = "EntitlementSourceColumnsContractPackageMap_ID", unique = true, nullable = false)
	private Long EntitlementSourceColumnsContractPackageMap_ID;
	
	@Column(name = "SourceColumnsPackage_ID", nullable = false, insertable = false, updatable = false)
	private Long SourceColumnsPackage_ID;
	
	@Column(name = "Contract_ID", nullable = false)
	private String Contract_ID;
	
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "Expiration_Date", nullable = true)
	private Date Expiration_Date;
	
	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "Last_Modification_Date", nullable = false)
	private Date Last_Modification_Date;
	
	@ManyToOne(fetch=FetchType.LAZY)
	@JoinColumn(name="SourceColumnsPackage_ID")
	private EntitlementSourceColumnsPackages entitlementSourceColumnsPackage;
	
	public EntitlementSourceColumnsContractPackageMap() {
		super();
	}

	@JsonProperty("EntitlementSourceColumnsContractPackageMap_ID")
	public Long getEntitlementSourceColumnsContractPackageMap_ID() {
		return EntitlementSourceColumnsContractPackageMap_ID;
	}

	@JsonProperty("EntitlementSourceColumnsContractPackageMap_ID")
	public void setEntitlementSourceColumnsContractPackageMap_ID(
			Long entitlementSourceColumnsContractPackageMap_ID) {
		EntitlementSourceColumnsContractPackageMap_ID = entitlementSourceColumnsContractPackageMap_ID;
	}

	@JsonProperty("SourceColumnsPackage_ID")
	public Long getSourceColumnsPackage_ID() {
		return SourceColumnsPackage_ID;
	}

	@JsonProperty("SourceColumnsPackage_ID")
	public void setSourceColumnsPackage_ID(Long sourceColumnsPackage_ID) {
		SourceColumnsPackage_ID = sourceColumnsPackage_ID;
	}

	@JsonProperty("Contract_ID")
	public String getContract_ID() {
		return Contract_ID;
	}

	@JsonProperty("Contract_ID")
	public void setContract_ID(String contract_ID) {
		Contract_ID = contract_ID;
	}

	@JsonProperty("Expiration_Date")
	public Date getExpiration_Date() {
		return Expiration_Date;
	}

	@JsonProperty("Expiration_Date")
	public void setExpiration_Date(Date expiration_Date) {
		Expiration_Date = expiration_Date;
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
	public EntitlementSourceColumnsPackages getEntitlementSourceColumnsPackage() {
		return entitlementSourceColumnsPackage;
	}

	@JsonIgnore
	public void setEntitlementSourceColumnsPackage(
			EntitlementSourceColumnsPackages entitlementSourceColumnsPackage) {
		this.entitlementSourceColumnsPackage = entitlementSourceColumnsPackage;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((EntitlementSourceColumnsContractPackageMap_ID == null) ? 0
						: EntitlementSourceColumnsContractPackageMap_ID
								.hashCode());
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
		EntitlementSourceColumnsContractPackageMap other = (EntitlementSourceColumnsContractPackageMap) obj;
		if (EntitlementSourceColumnsContractPackageMap_ID == null) {
			if (other.EntitlementSourceColumnsContractPackageMap_ID != null)
				return false;
		} else if (!EntitlementSourceColumnsContractPackageMap_ID
				.equals(other.EntitlementSourceColumnsContractPackageMap_ID))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "EntitlementSourceColumnsContractPackageMap [EntitlementSourceColumnsContractPackageMap_ID="
				+ EntitlementSourceColumnsContractPackageMap_ID
				+ ", SourceColumnsPackage_ID="
				+ SourceColumnsPackage_ID
				+ ", Contract_ID="
				+ Contract_ID
				+ ", Expiration_Date="
				+ Expiration_Date
				+ ", Last_Modification_Date="
				+ Last_Modification_Date
				+ ", entitlementSourceColumnsPackage="
				+ entitlementSourceColumnsPackage + "]";
	}

	@Override
	@JsonIgnore
	public Long getPid() {
		return EntitlementSourceColumnsContractPackageMap_ID;
	}

	@Override
	@JsonIgnore
	public void setPid(Long pid) {
		this.EntitlementSourceColumnsContractPackageMap_ID = pid;
		
	}
}
