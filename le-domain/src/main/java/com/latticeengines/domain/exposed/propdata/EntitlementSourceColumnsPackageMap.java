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
@Table(name = "EntitlementSourceColumnsPackageMap")
public class EntitlementSourceColumnsPackageMap implements HasPid{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "EntitlementSourceColumnsPackageMap_ID", unique = true, nullable = false)
    private Long EntitlementSourceColumnsPackageMap_ID;
    
    @Column(name = "SourceColumnsPackage_ID", nullable = false, insertable = false, updatable = false)
    private Long SourceColumnsPackage_ID;
    
    @Column(name = "IsDomainBased", nullable = false)
    private Boolean IsDomainBased;
    
    @Column(name = "Lookup_ID", nullable = false)
    private String Lookup_ID;
    
    @Column(name = "ColumnName", nullable = false)
    private String ColumnName;
    
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "Last_Modification_Date", nullable = false)
    private Date Last_Modification_Date;
    
    @ManyToOne(fetch=FetchType.LAZY)
    @JoinColumn(name="SourceColumnsPackage_ID")
    private EntitlementSourceColumnsPackages entitlementSourceColumnsPackage;
    
    public EntitlementSourceColumnsPackageMap() {
        super();
    }

    @JsonProperty("EntitlementSourceColumnsPackageMap_ID")
    public Long getEntitlementSourceColumnsPackageMap_ID() {
        return EntitlementSourceColumnsPackageMap_ID;
    }

    @JsonProperty("EntitlementSourceColumnsPackageMap_ID")
    public void setEntitlementSourceColumnsPackageMap_ID(
            Long entitlementSourceColumnsPackageMap_ID) {
        EntitlementSourceColumnsPackageMap_ID = entitlementSourceColumnsPackageMap_ID;
    }

    @JsonProperty("SourceColumnsPackage_ID")
    public Long getSourceColumnsPackage_ID() {
        return SourceColumnsPackage_ID;
    }

    @JsonProperty("SourceColumnsPackage_ID")
    public void setSourceColumnsPackage_ID(Long sourceColumnsPackage_ID) {
        SourceColumnsPackage_ID = sourceColumnsPackage_ID;
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

    @JsonProperty("ColumnName")
    public String getColumnName() {
        return ColumnName;
    }

    @JsonProperty("ColumnName")
    public void setColumnName(String columnName) {
        ColumnName = columnName;
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
                + ((EntitlementSourceColumnsPackageMap_ID == null) ? 0
                        : EntitlementSourceColumnsPackageMap_ID.hashCode());
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
        EntitlementSourceColumnsPackageMap other = (EntitlementSourceColumnsPackageMap) obj;
        if (EntitlementSourceColumnsPackageMap_ID == null) {
            if (other.EntitlementSourceColumnsPackageMap_ID != null)
                return false;
        } else if (!EntitlementSourceColumnsPackageMap_ID
                .equals(other.EntitlementSourceColumnsPackageMap_ID))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "EntitlementSourceColumnsPackageMap [EntitlementSourceColumnsPackageMap_ID="
                + EntitlementSourceColumnsPackageMap_ID
                + ", SourceColumnsPackage_ID="
                + SourceColumnsPackage_ID
                + ", IsDomainBased="
                + IsDomainBased
                + ", Lookup_ID="
                + Lookup_ID
                + ", ColumnName="
                + ColumnName
                + ", Last_Modification_Date="
                + Last_Modification_Date
                + ", entitlementSourceColumnsPackage="
                + entitlementSourceColumnsPackage + "]";
    }

    @Override
    @JsonIgnore
    public Long getPid() {
        return EntitlementSourceColumnsPackageMap_ID;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.EntitlementSourceColumnsPackageMap_ID = pid;
        
    }

}
