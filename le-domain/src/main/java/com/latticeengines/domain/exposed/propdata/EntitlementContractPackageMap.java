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
@Table(name = "EntitlementContractPackageMap")
public class EntitlementContractPackageMap  implements HasPid{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "EntitlementContractPackageMap_ID", unique = true, nullable = false)
    private Long EntitlementContractPackageMap_ID;
    
    @Column(name = "Package_ID", nullable = false, insertable = false, updatable = false)
    private Long Package_ID;
    
    @Column(name = "Contract_ID", nullable = false)
    private String Contract_ID;
    
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "Expiration_Date", nullable = true)
    private Date Expiration_Date;
    
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "Last_Modification_Date", nullable = false)
    private Date Last_Modification_Date;
    
    @ManyToOne(fetch=FetchType.LAZY)
    @JoinColumn(name="Package_ID")
    private EntitlementPackages entitlementPackage;
    
    public EntitlementContractPackageMap() {
        super();
    }

    @JsonProperty("EntitlementContractPackageMap_ID")
    public Long getEntitlementContractPackageMap_ID() {
        return EntitlementContractPackageMap_ID;
    }

    @JsonProperty("EntitlementContractPackageMap_ID")
    public void setEntitlementContractPackageMap_ID(
            Long entitlementContractPackageMap_ID) {
        EntitlementContractPackageMap_ID = entitlementContractPackageMap_ID;
    }

    @JsonProperty("Package_ID")
    public Long getPackage_ID() {
        return Package_ID;
    }

    @JsonProperty("Package_ID")
    public void setPackage_ID(Long package_ID) {
        Package_ID = package_ID;
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
    public EntitlementPackages getEntitlementPackage() {
        return entitlementPackage;
    }

    @JsonIgnore
    public void setEntitlementPackage(EntitlementPackages entitlementPackage) {
        this.entitlementPackage = entitlementPackage;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime
                * result
                + ((EntitlementContractPackageMap_ID == null) ? 0
                        : EntitlementContractPackageMap_ID.hashCode());
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
        EntitlementContractPackageMap other = (EntitlementContractPackageMap) obj;
        if (EntitlementContractPackageMap_ID == null) {
            if (other.EntitlementContractPackageMap_ID != null)
                return false;
        } else if (!EntitlementContractPackageMap_ID
                .equals(other.EntitlementContractPackageMap_ID))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "EntitlementContractPackageMap [EntitlementContractPackageMap_ID="
                + EntitlementContractPackageMap_ID
                + ", Package_ID="
                + Package_ID
                + ", Contract_ID="
                + Contract_ID
                + ", Expiration_Date="
                + Expiration_Date
                + ", Last_Modification_Date="
                + Last_Modification_Date
                + ", entitlementPackage=" + entitlementPackage + "]";
    }

    @Override
    @JsonIgnore
    public Long getPid() {
        return EntitlementContractPackageMap_ID;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.EntitlementContractPackageMap_ID = pid;
        
    }
}
