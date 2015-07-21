package com.latticeengines.domain.exposed.propdata;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@Entity
@Access(AccessType.FIELD)
@Table(name = "DomainFeatureTable")
public class DomainFeatureTable implements HasPid {

    @Id
    @Column(name = "DomainFeatureTable_ID", unique = true, nullable = false)
    private Long DomainFeatureTable_ID;
    
    @Column(name = "External_ID", nullable = false)
    private String External_ID;

    @Column(name = "Lookup_ID", nullable = false)
    private String Lookup_ID;
    
    @Column(name = "Provider_Name", nullable = true)
    private String Provider_Name;
    
    @Column(name = "Supports_Collection", nullable = false)
    private Boolean Supports_Collection;
    
    @Column(name = "Remote_Aggregation_Procedure", nullable = true)
    private String Remote_Aggregation_Procedure;
    
    @Column(name = "Local_Transfer_Procedure", nullable = true)
    private String Local_Transfer_Procedure;
    
    @Column(name = "Is_Active", nullable = false)
    private Boolean Is_Active;
    
    @Column(name = "Collection_Frequency", nullable = true)
    private Integer Collection_Frequency;
    
    public DomainFeatureTable() {
        super();
    }

    @JsonProperty("DomainFeatureTable_ID")
    public Long getDomainFeatureTable_ID() {
        return DomainFeatureTable_ID;
    }

    @JsonProperty("DomainFeatureTable_ID")
    public void setDomainFeatureTable_ID(Long domainFeatureTable_ID) {
        DomainFeatureTable_ID = domainFeatureTable_ID;
    }

    @JsonProperty("External_ID")
    public String getExternal_ID() {
        return External_ID;
    }

    @JsonProperty("External_ID")
    public void setExternal_ID(String external_ID) {
        External_ID = external_ID;
    }

    @JsonProperty("Lookup_ID")
    public String getLookup_ID() {
        return Lookup_ID;
    }

    @JsonProperty("Lookup_ID")
    public void setLookup_ID(String lookup_ID) {
        Lookup_ID = lookup_ID;
    }

    @JsonProperty("Provider_Name")
    public String getProvider_Name() {
        return Provider_Name;
    }

    @JsonProperty("Provider_Name")
    public void setProvider_Name(String provider_Name) {
        Provider_Name = provider_Name;
    }

    @JsonProperty("Supports_Collection")
    public Boolean getSupports_Collection() {
        return Supports_Collection;
    }

    @JsonProperty("Supports_Collection")
    public void setSupports_Collection(Boolean supports_Collection) {
        Supports_Collection = supports_Collection;
    }

    @JsonProperty("Remote_Aggregation_Procedure")
    public String getRemote_Aggregation_Procedure() {
        return Remote_Aggregation_Procedure;
    }

    @JsonProperty("Remote_Aggregation_Procedure")
    public void setRemote_Aggregation_Procedure(String remote_Aggregation_Procedure) {
        Remote_Aggregation_Procedure = remote_Aggregation_Procedure;
    }

    @JsonProperty("Local_Transfer_Procedure")
    public String getLocal_Transfer_Procedure() {
        return Local_Transfer_Procedure;
    }

    @JsonProperty("Local_Transfer_Procedure")
    public void setLocal_Transfer_Procedure(String local_Transfer_Procedure) {
        Local_Transfer_Procedure = local_Transfer_Procedure;
    }

    @JsonProperty("Is_Active")
    public Boolean getIs_Active() {
        return Is_Active;
    }

    @JsonProperty("Is_Active")
    public void setIs_Active(Boolean is_Active) {
        Is_Active = is_Active;
    }

    @JsonProperty("Collection_Frequency")
    public Integer getCollection_Frequency() {
        return Collection_Frequency;
    }

    @JsonProperty("Collection_Frequency")
    public void setCollection_Frequency(Integer collection_Frequency) {
        Collection_Frequency = collection_Frequency;
    }

    @Override
    @JsonIgnore
    public Long getPid() {
        return DomainFeatureTable_ID;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.DomainFeatureTable_ID = pid;
        
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime
                * result
                + ((DomainFeatureTable_ID == null) ? 0 : DomainFeatureTable_ID
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
        DomainFeatureTable other = (DomainFeatureTable) obj;
        if (DomainFeatureTable_ID == null) {
            if (other.DomainFeatureTable_ID != null)
                return false;
        } else if (!DomainFeatureTable_ID.equals(other.DomainFeatureTable_ID))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "DomainFeatureTable [DomainFeatureTable_ID="
                + DomainFeatureTable_ID + ", External_ID=" + External_ID
                + ", Lookup_ID=" + Lookup_ID + ", Provider_Name="
                + Provider_Name + ", Supports_Collection="
                + Supports_Collection + ", Remote_Aggregation_Procedure="
                + Remote_Aggregation_Procedure + ", Local_Transfer_Procedure="
                + Local_Transfer_Procedure + ", Is_Active=" + Is_Active
                + ", Collection_Frequency=" + Collection_Frequency + "]";
    }

}
