package com.latticeengines.domain.exposed.metadata.datastore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "StorageType")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = DynamoDataUnit.class, name = "Dynamo"), //
        @JsonSubTypes.Type(value = RedshiftDataUnit.class, name = "Redshift"), //
        @JsonSubTypes.Type(value = S3DataUnit.class, name = "S3"), //
        @JsonSubTypes.Type(value = PrestoDataUnit.class, name = "Presto"), //
        @JsonSubTypes.Type(value = AthenaDataUnit.class, name = "Athena"), //
        @JsonSubTypes.Type(value = HdfsDataUnit.class, name = "Hdfs"), //
        @JsonSubTypes.Type(value = ElasticSearchDataUnit.class, name = "ElasticSearch")
})
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public abstract class DataUnit {

    @JsonProperty("PartitionKeys")
    private List<String> partitionKeys;

    @JsonProperty("TypedPartitionKeys")
    private List<Pair<String, String>> typedPartitionKeys;

    @JsonProperty("Tenant")
    private String tenant;

    @JsonProperty("Name")
    private String name;

    @JsonProperty("Count")
    private Long count;

    @JsonProperty("DataFormat")
    private DataFormat dataFormat;

    @JsonProperty("Roles")
    private List<Role> roles;

    @JsonProperty("DataTemplateId")
    private String dataTemplateId;

    @JsonProperty("retentionPolicy")
    private String retentionPolicy;

    @JsonProperty("updated")
    private Date updated;

    public abstract StorageType getStorageType();

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public DataFormat getDataFormat() {
        return dataFormat;
    }

    public void setDataFormat(DataFormat dataFormat) {
        this.dataFormat = dataFormat;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public List<Role> getRoles() {
        return roles;
    }

    public void setRoles(List<Role> roles) {
        this.roles = roles;
    }

    public String getDataTemplateId() {
        return dataTemplateId;
    }

    public void setDataTemplateId(String dataTemplateId) {
        this.dataTemplateId = dataTemplateId;
    }

    public List<Pair<String, String>> getTypedPartitionKeys() {
        return typedPartitionKeys;
    }

    public void setTypedPartitionKeys(Collection<Pair<String, String>> typedPartitionKeys) {
        this.typedPartitionKeys = new ArrayList<>(typedPartitionKeys);
    }

    public String getRetentionPolicy() {
        return retentionPolicy;
    }

    public void setRetentionPolicy(String retentionPolicy) {
        this.retentionPolicy = retentionPolicy;
    }

    public Date getUpdated() {
        return updated;
    }

    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    public enum StorageType {
        Dynamo, Hdfs, Redshift, S3,ElasticSearch, Presto, Athena, Unknown;

        @JsonCreator
        public static StorageType safeValueOf(String string) {
            try {
                return StorageType.valueOf(string);
            } catch (IllegalArgumentException e) {
                return Unknown;
            }
        }
    }

    public enum DataFormat {
        AVRO, PARQUET, CSV, ZIP, UNKOWN;

        @JsonCreator
        public static DataFormat safeValueOf(String string) {
            try {
                return DataFormat.valueOf(string);
            } catch (IllegalArgumentException e) {
                return UNKOWN;
            }
        }
    }

    public enum Role {
        Master, Import, Snapshot, Serving, Unknown;

        @JsonCreator
        public static Role safeValueOf(String string) {
            try {
                return Role.valueOf(string);
            } catch (IllegalArgumentException e) {
                return Unknown;
            }
        }
    }
}


