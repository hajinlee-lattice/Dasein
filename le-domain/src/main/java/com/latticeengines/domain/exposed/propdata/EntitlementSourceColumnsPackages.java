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
@Table(name = "EntitlementSourceColumnsPackages")
public class EntitlementSourceColumnsPackages implements HasPid{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "SourceColumnsPackage_ID", unique = true, nullable = false)
    private Long SourceColumnsPackage_ID;
    
    @Column(name = "SourceColumnsPackageName", nullable = false)
    private String SourceColumnsPackageName;
    
    @Column(name = "SourceColumnsPackageDescription", nullable = true)
    private String SourceColumnsPackageDescription;
    
    @Column(name = "Is_Default", nullable = false)
    private Boolean Is_Default;
    
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "Last_Modification_Date", nullable = false)
    private Date Last_Modification_Date;
    
    public EntitlementSourceColumnsPackages() {
        super();
    }

    @JsonProperty("SourceColumnsPackage_ID")
    public Long getSourceColumnsPackage_ID() {
        return SourceColumnsPackage_ID;
    }

    @JsonProperty("SourceColumnsPackage_ID")
    public void setSourceColumnsPackage_ID(Long sourceColumnsPackage_ID) {
        SourceColumnsPackage_ID = sourceColumnsPackage_ID;
    }

    @JsonProperty("SourceColumnsPackageName")
    public String getSourceColumnsPackageName() {
        return SourceColumnsPackageName;
    }

    @JsonProperty("SourceColumnsPackageName")
    public void setSourceColumnsPackageName(String sourceColumnsPackageName) {
        SourceColumnsPackageName = sourceColumnsPackageName;
    }

    @JsonProperty("SourceColumnsPackageDescription")
    public String getSourceColumnsPackageDescription() {
        return SourceColumnsPackageDescription;
    }

    @JsonProperty("SourceColumnsPackageDescription")
    public void setSourceColumnsPackageDescription(
            String sourceColumnsPackageDescription) {
        SourceColumnsPackageDescription = sourceColumnsPackageDescription;
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
                + ((SourceColumnsPackage_ID == null) ? 0
                        : SourceColumnsPackage_ID.hashCode());
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
        EntitlementSourceColumnsPackages other = (EntitlementSourceColumnsPackages) obj;
        if (SourceColumnsPackage_ID == null) {
            if (other.SourceColumnsPackage_ID != null)
                return false;
        } else if (!SourceColumnsPackage_ID
                .equals(other.SourceColumnsPackage_ID))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "EntitlementSourceColumnsPackages [SourceColumnsPackage_ID="
                + SourceColumnsPackage_ID + ", SourceColumnsPackageName="
                + SourceColumnsPackageName
                + ", SourceColumnsPackageDescription="
                + SourceColumnsPackageDescription + ", Is_Default="
                + Is_Default + ", Last_Modification_Date="
                + Last_Modification_Date + "]";
    }

    @Override
    @JsonIgnore
    public Long getPid() {
        return SourceColumnsPackage_ID;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.SourceColumnsPackage_ID = pid;
        
    }
}
