package com.latticeengines.domain.exposed.datacloud.manage;

import static com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn.JSON_PATH;
import static com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn.PRIME_COLUMN_ID;
import static com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn.TABLE_NAME;

import java.util.List;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

@Entity
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
@Access(AccessType.FIELD)
@Table(name = TABLE_NAME, uniqueConstraints = {
        @UniqueConstraint(name = "UQ_PRIME_COLUMN_ID", columnNames = { PRIME_COLUMN_ID }), //
        @UniqueConstraint(name = "UQ_PRIME_COLUMN_JSON_PATH", columnNames = { JSON_PATH })
})
public class PrimeColumn implements MetadataColumn {

    public static final String TABLE_NAME = "PrimeColumn";
    public static final String PRIME_COLUMN_ID = "PrimeColumnId";
    public static final String JSON_PATH = "JsonPath";

    @Id
    @JsonIgnore
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("primeColumnId")
    @Column(name = PRIME_COLUMN_ID, nullable = false, length = 64)
    private String primeColumnId;

    @JsonProperty("jsonPath")
    @Column(name = JSON_PATH, nullable = false, length = 256)
    private String jsonPath;

    @JsonProperty("displayName")
    @Column(name = "DisplayName", nullable = false, length = 256)
    private String displayName;

    @JsonProperty("description")
    @Column(name = "Description", length = 1024)
    private String description;

    @JsonProperty("javaClass")
    @Column(name = "JavaClass", nullable = false, length = 16)
    private String javaClass;

    @JsonProperty("example")
    @Column(name = "Example", nullable = true, length = 4000)
    private String example;

    @JsonIgnore
    @LazyCollection(LazyCollectionOption.TRUE)
    @OneToMany(mappedBy = "primeColumn")
    private List<DataBlockElement> dataBlocks;

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getPrimeColumnId() {
        return primeColumnId;
    }

    public void setPrimeColumnId(String primeColumnId) {
        this.primeColumnId = primeColumnId;
    }

    public List<DataBlockElement> getDataBlocks() {
        return dataBlocks;
    }

    public void setDataBlocks(List<DataBlockElement> dataBlocks) {
        this.dataBlocks = dataBlocks;
    }

    public String getJsonPath() {
        return jsonPath;
    }

    public void setJsonPath(String jsonPath) {
        this.jsonPath = jsonPath;
    }

    public String getExample() { return example; }

    public void setExample(String example) { this.example = example; }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getJavaClass() {
        return javaClass;
    }

    public void setJavaClass(String javaClass) {
        this.javaClass = javaClass;
    }

    private PrimeColumn(){}

    public PrimeColumn(String attrName, String displayName, String jsonPath, String example) {
        this.primeColumnId = attrName;
        this.displayName = displayName;
        this.jsonPath = jsonPath;
        this.example = example;
    }

    public PrimeColumn(String attrName, String displayName, String jsonPath, String javaClass, String example) {
        this.primeColumnId = attrName;
        this.displayName = displayName;
        this.jsonPath = jsonPath;
        this.javaClass = javaClass;
        this.example = example;
    }

    public PrimeColumn(String attrName, String displayName, String jsonPath, String javaClass, String description, String example) {
        this.primeColumnId = attrName;
        this.displayName = displayName;
        this.jsonPath = jsonPath;
        this.javaClass = javaClass;
        this.example = example;
        this.description = description;
    }

    public String getAttrName() {
        return primeColumnId;
    }

    public void setAttrName(String attrName) {
        this.primeColumnId = attrName;
    }

    @Override
    public ColumnMetadata toColumnMetadata() {
        ColumnMetadata cm = new ColumnMetadata();
        cm.setAttrName(getColumnId());
        cm.setDisplayName(getDisplayName());
        cm.setJavaClass(String.class.getSimpleName());
        return cm;
    }

    @Override
    public String getColumnId() {
        return primeColumnId;
    }

    @Override
    public boolean containsTag(String tag) {
        return false;
    }

}
