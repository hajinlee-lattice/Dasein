package com.latticeengines.domain.exposed.dataplatform;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringTokenUtils;

@Entity
@Table(name = "MODEL")
public class Model implements HasName, HasPid, HasId<String> {

    private Long pid;
    private String id;
    private String name;
    private String dataHdfsPath;
    private String metadataHdfsPath;
    private String schemaHdfsPath;
    private String modelHdfsDir;
    private List<String> features;
    private List<String> targets;
    private List<String> keyCols;
    private ModelDefinition modelDefinition;
    private List<Job> jobs = new ArrayList<Job>();
    private String dataFormat;
    private String customer;
    private String table;
    private String metadataTable;
    private String provenanceProperties;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @JsonIgnore
    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    @JsonProperty("name")
    @Column(name = "NAME")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("model_definition")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_MODEL_DEF_ID")
    public ModelDefinition getModelDefinition() {
        return modelDefinition;
    }

    @JsonProperty("model_definition")
    public void setModelDefinition(ModelDefinition modelDef) {
        this.modelDefinition = modelDef;
        if (this.modelDefinition != null) {
            this.modelDefinition.addModel(this);
        }
    }

    @Column(name = "FEATURES", length = 65535)
    @JsonIgnore
    public String getFeatures() {
        return StringTokenUtils.listToString(this.features);
    }

    /**
     * @param featuresString
     *            - string of comma-separated features
     **/
    @JsonIgnore
    public void setFeatures(String featuresString) {
        this.features = StringTokenUtils.stringToList(featuresString);
    }

    @Transient
    @JsonProperty("features")
    public List<String> getFeaturesList() {
        return features;
    }

    @JsonProperty("features")
    public void setFeaturesList(List<String> features) {
        this.features = features;
    }

    @Transient
    @JsonProperty("targets")
    public List<String> getTargetsList() {
        return targets;
    }

    @JsonIgnore
    @Column(name = "TARGETS")
    public String getTargets() {
        return StringTokenUtils.listToString(this.targets);
    }

    @JsonProperty("targets")
    public void setTargetsList(List<String> targets) {
        this.targets = targets;
    }

    @JsonIgnore
    public void setTargets(String targets) {
        this.targets = StringTokenUtils.stringToList(targets);
    }

    @JsonProperty("metadata")
    @Column(name = "METADATA_HDFS_PATH")
    public String getMetadataHdfsPath() {
        return metadataHdfsPath;
    }

    @JsonProperty("metadata")
    public void setMetadataHdfsPath(String metadataHdfsPath) {
        this.metadataHdfsPath = metadataHdfsPath;
    }

    @JsonProperty("data")
    @Column(name = "DATA_HDFS_PATH")
    public String getDataHdfsPath() {
        return dataHdfsPath;
    }

    @JsonProperty("data")
    public void setDataHdfsPath(String dataHdfsPath) {
        this.dataHdfsPath = dataHdfsPath;
    }

    @JsonProperty("schema")
    @Column(name = "SCHEMA_HDFS_PATH")
    public String getSchemaHdfsPath() {
        return schemaHdfsPath;
    }

    @JsonProperty("schema")
    public void setSchemaHdfsPath(String schemaHdfsPath) {
        this.schemaHdfsPath = schemaHdfsPath;
    }

    @JsonProperty("model_dir_data")
    @Column(name = "MODEL_HDFS_DIR")
    public String getModelHdfsDir() {
        return modelHdfsDir;
    }

    @JsonProperty("model_dir_data")
    public void setModelHdfsDir(String modelHdfsDir) {
        this.modelHdfsDir = modelHdfsDir;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    @JsonIgnore
    @Column(name = "MODEL_ID")
    public String getId() {
        return id;
    }

    @Override
    @JsonIgnore
    public void setId(String id) {
        this.id = id;
    }

    public void addJob(Job job) {
        job.setModel(this);
        jobs.add(job);
    }

    @JsonProperty("data_format")
    @Column(name = "DATA_FORMAT")
    public String getDataFormat() {
        return dataFormat;
    }

    @JsonProperty("data_format")
    public void setDataFormat(String dataFormat) {
        this.dataFormat = dataFormat;
    }

    @JsonProperty("customer")
    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @Column(name = "CUSTOMER")
    public String getCustomer() {
        return customer;
    }

    @JsonProperty("table")
    @Column(name = "TABLE_NAME")
    public String getTable() {
        return table;
    }

    @JsonProperty("table")
    public void setTable(String table) {
        this.table = table;
    }

    @JsonProperty("metadata_table")
    @Column(name = "METADATA_TABLE")
    public String getMetadataTable() {
        return metadataTable;
    }

    @JsonProperty("metadata_table")
    public void setMetadataTable(String metadataTable) {
        this.metadataTable = metadataTable;
    }

    @JsonIgnore
    @Transient
    public String getSampleHdfsPath() {
        return getDataHdfsPath() + "/samples";
    }

    @Transient
    @JsonProperty("key_columns")
    public List<String> getKeyColsList() {
        return keyCols;
    }

    @Column(name = "KEYCOLS", length = 500)
    @JsonIgnore
    public String getKeyCols() {
        return StringTokenUtils.listToString(this.keyCols);
    }

    @JsonProperty("key_columns")
    public void setKeyCols(List<String> keyCols) {
        this.keyCols = keyCols;
    }

    @JsonIgnore
    public void setKeyCols(String keyCols) {
        this.keyCols = StringTokenUtils.stringToList(keyCols);
    }

    @JsonProperty("provenance_properties")
    public void setProvenanceProperties(String provenanceProperties) {
        this.provenanceProperties = provenanceProperties;
    }

    @JsonProperty("provenance_properties")
    @Column(name = "PROVENANCE_PROPERTIES")
    public String getProvenanceProperties() {
        return this.provenanceProperties;
    }

    /**
     * http://docs.jboss.org/hibernate/core/4.0/manual/en-US/html/persistent-
     * classes.html#persistent-classes-equalshashcode
     */
    @Override
    public int hashCode() {
        int result;
        result = getId().hashCode();
        result = 29 * result + getName().hashCode();
        return result;
    }

    /**
     * http://docs.jboss.org/hibernate/core/4.0/manual/en-US/html/persistent-
     * classes.html#persistent-classes-equalshashcode
     * 
     * right now, it only perform a partially shallow comparison due to
     * efficiency reason. Collection object is compared, but composite domain
     * object is not compared. If composite domain object needs to be compared,
     * its equals() method has to be explicitly called.
     */
    @Override
    public boolean equals(Object obj) {

        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!obj.getClass().equals(this.getClass()))
            return false;

        Model model = (Model) obj;

        return new EqualsBuilder().append(pid, model.getPid()).append(id, model.getId()).append(name, model.getName())
                .append(dataHdfsPath, model.getDataHdfsPath()).append(metadataHdfsPath, model.getMetadataHdfsPath())
                .append(schemaHdfsPath, model.getSchemaHdfsPath()).append(modelHdfsDir, model.getModelHdfsDir())
                .append(features, model.getFeaturesList()).append(targets, model.getTargetsList())
                .append(keyCols, model.getKeyColsList()).append(dataFormat, model.getDataFormat())
                .append(customer, model.getCustomer()).append(table, model.getTable())
                .append(metadataTable, model.getMetadataTable()).isEquals();

    }
}
