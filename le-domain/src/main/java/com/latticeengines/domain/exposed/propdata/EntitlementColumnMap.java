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
@Table(name = "EntitlementColumnMap")
public class EntitlementColumnMap implements HasPid, HasPackageId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "EntitlementColumnMap_ID", unique = true, nullable = false)
    private Long EntitlementColumnMap_ID;
    
    @Column(name = "Package_ID", nullable = false, insertable = false, updatable = false)
    private Long Package_ID;

    @Column(name = "ColumnCalculation_ID", nullable = false)
    private Long ColumnCalculation_ID;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "Last_Modification_Date", nullable = false)
    private Date Last_Modification_Date;
    
    public EntitlementColumnMap() {
        super();
    }
    
    @ManyToOne(fetch=FetchType.LAZY)
    @JoinColumn(name="Package_ID")
    private EntitlementPackages entitlementPackage;

    @JsonIgnore
    public EntitlementPackages getEntitlementPackage() {
        return entitlementPackage;
    }

    @JsonIgnore
    public void setEntitlementPackage(EntitlementPackages entitlementPackage) {
        this.entitlementPackage = entitlementPackage;
    }

    @JsonProperty("EntitlementColumnMap_ID")
    public Long getEntitlementColumnMap_ID() {
        return EntitlementColumnMap_ID;
    }

    @JsonProperty("EntitlementColumnMap_ID")
    public void setEntitlementColumnMap_ID(Long entitlementColumnMap_ID) {
        EntitlementColumnMap_ID = entitlementColumnMap_ID;
    }

    @JsonProperty("Package_ID")
    public Long getPackage_ID() {
        return Package_ID;
    }

    @JsonProperty("Package_ID")
    public void setPackage_ID(Long package_ID) {
        Package_ID = package_ID;
    }

    @JsonProperty("ColumnCalculation_ID")
    public Long getColumnCalculation_ID() {
        return ColumnCalculation_ID;
    }

    @JsonProperty("ColumnCalculation_ID")
    public void setColumnCalculation_ID(Long columnCalculation_ID) {
        ColumnCalculation_ID = columnCalculation_ID;
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
                + ((EntitlementColumnMap_ID == null) ? 0
                        : EntitlementColumnMap_ID.hashCode());
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
        EntitlementColumnMap other = (EntitlementColumnMap) obj;
        if (EntitlementColumnMap_ID == null) {
            if (other.EntitlementColumnMap_ID != null)
                return false;
        } else if (!EntitlementColumnMap_ID
                .equals(other.EntitlementColumnMap_ID))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "EntitlementColumnMap [EntitlementColumnMap_ID="
                + EntitlementColumnMap_ID + ", Package_ID=" + Package_ID
                + ", ColumnCalculation_ID=" + ColumnCalculation_ID
                + ", Last_Modification_Date=" + Last_Modification_Date
                + ", entitlementPackage=" + entitlementPackage + "]";
    }

    @Override
    @JsonIgnore
    public Long getPid() {
        return this.EntitlementColumnMap_ID;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.EntitlementColumnMap_ID = pid;
        
    }

    @Override
    @JsonIgnore
    public Long getPackageId() { return getPackage_ID(); }

    @Override
    @JsonIgnore
    public void setPackageId(Long packageId) { setPackage_ID(packageId); }
}
