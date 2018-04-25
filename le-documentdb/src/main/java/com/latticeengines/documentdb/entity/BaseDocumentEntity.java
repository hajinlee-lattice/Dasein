package com.latticeengines.documentdb.entity;


import static com.latticeengines.documentdb.entity.BaseDocumentEntity.JSON_NODE_TYPE;
import static com.latticeengines.documentdb.entity.BaseDocumentEntity.JSON_TYPE;
import static javax.persistence.TemporalType.TIMESTAMP;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.EntityListeners;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.Temporal;

import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vladmihalcea.hibernate.type.array.IntArrayType;
import com.vladmihalcea.hibernate.type.array.StringArrayType;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonNodeBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonNodeStringType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@TypeDefs({
        @TypeDef(name = "string-array", typeClass = StringArrayType.class),
        @TypeDef(name = "int-array", typeClass = IntArrayType.class),
        @TypeDef(name = JSON_TYPE, typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class),
        @TypeDef(name = "jsonb-node", typeClass = JsonNodeBinaryType.class),
        @TypeDef(name = JSON_NODE_TYPE, typeClass = JsonNodeStringType.class),
})
@MappedSuperclass
@EntityListeners(AuditingEntityListener.class)
public abstract class BaseDocumentEntity<T> implements DocumentEntity<T> {

    // column definitions
    static final String JSON_COLUMN = "JSON";

    // types
    static final String JSON_TYPE = "json";
    static final String JSON_NODE_TYPE = "json-node";

    @Id
    @JsonProperty("UUID")
    @Column(name = "UUID", length = 36, nullable = false)
    private String uuid;

    @JsonIgnore
    @Type(type = JSON_TYPE)
    @Column(name = "Document", columnDefinition = JSON_COLUMN)
    protected T document;

    @CreatedDate
    @Temporal(TIMESTAMP)
    @JsonProperty("CreatedDate")
    @Column(name = "CreatedDate", nullable = false)
    private Date createdDate;

    @LastModifiedDate
    @Temporal(TIMESTAMP)
    @JsonProperty("LastModifiedDate")
    @Column(name = "LastModifiedDate", nullable = false)
    private Date lastModifiedDate;

    @Override
    public String getUuid() {
        return uuid;
    }

    @Override
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    // managed by jpa auditing
    private void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    public Date getLastModifiedDate() {
        return lastModifiedDate;
    }

    // managed by jpa auditing
    private void setLastModifiedDate(Date lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
    }

    @Override
    public T getDocument() {
        return document;
    }

    @Override
    public void setDocument(T dto) {
        document = dto;
    }

}
