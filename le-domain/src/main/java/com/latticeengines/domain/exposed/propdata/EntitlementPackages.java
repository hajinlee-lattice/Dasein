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
@Table(name = "EntitlementPackages")
public class EntitlementPackages  implements HasPid, HasPackageName{
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "Package_ID", unique = true, nullable = false)
    private Long Package_ID;
    
    @Column(name = "packageName", nullable = false)
    private String packageName;
    
    @Column(name = "PackageDescription", nullable = true)
    private String PackageDescription;
    
    @Column(name = "Is_Default", nullable = false)
    private Boolean Is_Default;
    
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "Last_Modification_Date", nullable = false)
    private Date Last_Modification_Date;
    
    public EntitlementPackages(){
        super();
    }

    @JsonProperty("Package_ID")
    public Long getPackage_ID() {
        return Package_ID;
    }

    @JsonProperty("Package_ID")
    public void setPackage_ID(Long package_ID) {
        Package_ID = package_ID;
    }

    @JsonProperty("packageName")
    public String getPackageName() {
        return packageName;
    }

    @JsonProperty("packageName")
    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    @JsonProperty("PackageDescription")
    public String getPackageDescription() {
        return PackageDescription;
    }

    @JsonProperty("PackageDescription")
    public void setPackageDescription(String packageDescription) {
        PackageDescription = packageDescription;
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
        result = prime * result
                + ((Package_ID == null) ? 0 : Package_ID.hashCode());
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
        EntitlementPackages other = (EntitlementPackages) obj;
        if (Package_ID == null) {
            if (other.Package_ID != null)
                return false;
        } else if (!Package_ID.equals(other.Package_ID))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "EntitlementPackages [Package_ID=" + Package_ID
                + ", packageName=" + packageName + ", PackageDescription="
                + PackageDescription + ", Is_Default=" + Is_Default
                + ", Last_Modification_Date=" + Last_Modification_Date + "]";
    }

    @Override
    @JsonIgnore
    public Long getPid() {
        return Package_ID;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.Package_ID = pid;
        
    }
}
