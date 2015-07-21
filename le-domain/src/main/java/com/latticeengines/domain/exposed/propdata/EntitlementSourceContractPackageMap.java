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
@Table(name = "EntitlementSourceContractPackageMap")
public class EntitlementSourceContractPackageMap implements HasPid{

    public EntitlementSourceContractPackageMap() {
        super();
    }
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "EntitlementSourceContractPackageMap_ID", unique = true, nullable = false)
    private Long EntitlementSourceContractPackageMap_ID;
    
    @Column(name = "SourcePackage_ID", nullable = false, insertable = false, updatable = false)
    private Long SourcePackage_ID;
    
    @Column(name = "Contract_ID", nullable = false)
    private String Contract_ID;
    
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "Expiration_Date", nullable = true)
    private Date Expiration_Date;
    
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "Last_Modification_Date", nullable = false)
    private Date Last_Modification_Date;
    
    @ManyToOne(fetch=FetchType.LAZY)
    @JoinColumn(name="SourcePackage_ID")
    private EntitlementSourcePackages entitlementSourcePackage;

    @JsonProperty("EntitlementSourceContractPackageMap_ID")
    public Long getEntitlementSourceContractPackageMap_ID() {
        return EntitlementSourceContractPackageMap_ID;
    }

    @JsonProperty("EntitlementSourceContractPackageMap_ID")
    public void setEntitlementSourceContractPackageMap_ID(
            Long entitlementSourceContractPackageMap_ID) {
        EntitlementSourceContractPackageMap_ID = entitlementSourceContractPackageMap_ID;
    }

    @JsonProperty("SourcePackage_ID")
    public Long getSourcePackage_ID() {
        return SourcePackage_ID;
    }

    @JsonProperty("SourcePackage_ID")
    public void setSourcePackage_ID(Long sourcePackage_ID) {
        SourcePackage_ID = sourcePackage_ID;
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
                + ((EntitlementSourceContractPackageMap_ID == null) ? 0
                        : EntitlementSourceContractPackageMap_ID.hashCode());
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
        EntitlementSourceContractPackageMap other = (EntitlementSourceContractPackageMap) obj;
        if (EntitlementSourceContractPackageMap_ID == null) {
            if (other.EntitlementSourceContractPackageMap_ID != null)
                return false;
        } else if (!EntitlementSourceContractPackageMap_ID
                .equals(other.EntitlementSourceContractPackageMap_ID))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "EntitlementSourceContractPackageMap [EntitlementSourceContractPackageMap_ID="
                + EntitlementSourceContractPackageMap_ID
                + ", SourcePackage_ID="
                + SourcePackage_ID
                + ", Contract_ID="
                + Contract_ID
                + ", Expiration_Date="
                + Expiration_Date
                + ", Last_Modification_Date="
                + Last_Modification_Date
                + ", entitlementSourcePackage="
                + entitlementSourcePackage
                + "]";
    }

    @Override
    @JsonIgnore
    public Long getPid() {
        return EntitlementSourceContractPackageMap_ID;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.EntitlementSourceContractPackageMap_ID = pid;
        
    }

}
