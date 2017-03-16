package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;


@Entity
@Access(AccessType.FIELD)
@Table(name = "LatticeIdStrategy")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LatticeIdStrategy implements HasPid, Serializable {

    private static final long serialVersionUID = 2146733373582565334L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "Strategy", nullable = false, length = 100)
    private String strategy;

    @Column(name = "IdName", nullable = false, length = 100)
    private String idName;

    @Enumerated(EnumType.STRING)
    @Column(name = "IdType", nullable = false, length = 100)
    private IdType idType;

    @Enumerated(EnumType.STRING)
    @Column(name = "Entity", nullable = false, length = 100)
    private Entity entity;

    @Column(name = "KeySet", nullable = false, length = 100)
    private String keySet;

    @Column(name = "Attrs", nullable = false, length = 200)
    private String attrs;

    @Column(name = "CheckObsolete", nullable = false)
    private boolean checkObsolete;

    @Column(name = "MergeDup", nullable = false)
    private boolean mergeDup;

    @Transient
    private Map<String, List<String>> keyMap;

    @SuppressWarnings("unchecked")
    public void addKeyMap(String keySet, String attrs) {
        if (keyMap == null) {
            keyMap = new HashMap<>();
        }
        List<String> attrList = Arrays.asList(attrs.split("\\|\\|"));
        keyMap.put(keySet, attrList);
    }

    public void copyBasicInfo(LatticeIdStrategy obj) {
        pid = obj.getPid();
        strategy = obj.getStrategy();
        idName = obj.getIdName();
        idType = obj.getIdType();
        entity = obj.getEntity();
        checkObsolete = obj.isCheckObsolete();
        mergeDup = obj.isMergeDup();
    }

    @Override
    @JsonIgnore
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("Strategy")
    public String getStrategy() {
        return strategy;
    }

    @JsonProperty("Strategy")
    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    @JsonProperty("IdName")
    public String getIdName() {
        return idName;
    }

    @JsonProperty("IdName")
    public void setIdName(String idName) {
        this.idName = idName;
    }

    @JsonProperty("IdType")
    public IdType getIdType() {
        return idType;
    }

    @JsonProperty("IdType")
    public void setIdType(IdType idType) {
        this.idType = idType;
    }

    @JsonProperty("Entity")
    public Entity getEntity() {
        return entity;
    }

    @JsonProperty("Entity")
    public void setEntity(Entity entity) {
        this.entity = entity;
    }

    @JsonProperty("KeySet")
    public String getKeySet() {
        return keySet;
    }

    @JsonProperty("KeySet")
    public void setKeySet(String keySet) {
        this.keySet = keySet;
    }

    @JsonProperty("Attrs")
    public String getAttrs() {
        return attrs;
    }

    @JsonProperty("Attrs")
    public void setAttrs(String attrs) {
        this.attrs = attrs;
    }

    @JsonProperty("CheckObsolete")
    public boolean isCheckObsolete() {
        return checkObsolete;
    }

    @JsonProperty("CheckObsolete")
    public void setCheckObsolete(boolean checkObsolete) {
        this.checkObsolete = checkObsolete;
    }

    @JsonProperty("MergeDup")
    public boolean isMergeDup() {
        return mergeDup;
    }

    @JsonProperty("MergeDup")
    public void setMergeDup(boolean mergeDup) {
        this.mergeDup = mergeDup;
    }

    public Map<String, List<String>> getKeyMap() {
        return keyMap;
    }

    public enum IdType {
        LONG, UUID
    }

    public enum Entity {
        ACCOUNT
    }
}
