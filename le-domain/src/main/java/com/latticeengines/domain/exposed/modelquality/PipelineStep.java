package com.latticeengines.domain.exposed.modelquality;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang.StringUtils;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

import edu.emory.mathcs.backport.java.util.Arrays;

/**
 * 
 * @startuml
 *
 */
@Entity
@Table(name = "MODELQUALITY_PIPELINE_STEP")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class PipelineStep implements HasName, HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    @JsonProperty("Name")
    private String name;
    
    @Column(name = "MAIN_CLASS_NAME", nullable = false)
    @JsonProperty("MainClassName")
    private String mainClassName;
    
    @Column(name = "OPERATES_ON_COLUMNS")
    private String operatesOnColumns;

    @JsonProperty("ColumnTransformFilePath")
    @Column(name = "SCRIPT", unique = true, nullable = false)
    private String script;

    @ManyToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL, mappedBy = "pipelineSteps")
    @JsonIgnore
    private List<Pipeline> pipelines = new ArrayList<>();

    @JsonProperty("pipeline_property_defs")
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER, mappedBy = "pipelineStep")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SELECT)
    private List<PipelinePropertyDef> pipelinePropertyDefs = new ArrayList<>();
    
    @JsonProperty("KeyWhenSortingByAscending")
    @Transient
    private int sortKey;
    
    @JsonProperty("UniqueColumnTransformName")
    @Transient
    private String uniqueColumnTransformName;
    
    @Column(name = "NAMED_PARAMS_TO_INIT")
    private String namedParameterListToInit;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public void addPipeline(Pipeline pipeline) {
        pipelines.add(pipeline);
    }

    public List<Pipeline> getPipelines() {
        return pipelines;
    }

    public List<PipelinePropertyDef> getPipelinePropertyDefs() {
        return pipelinePropertyDefs;
    }

    public void setPipelinePropertyDefs(List<PipelinePropertyDef> pipelinePropertyDefs) {
        this.pipelinePropertyDefs = pipelinePropertyDefs;
    }

    public void addPipelinePropertyDef(PipelinePropertyDef pipelinePropertyDef) {
        pipelinePropertyDefs.add(pipelinePropertyDef);
        pipelinePropertyDef.setPipelineStep(this);
    }

    public void setPipeline(List<Pipeline> pipelines) {
        this.pipelines = pipelines;
    }

    public String getMainClassName() {
        return mainClassName;
    }

    public void setMainClassName(String mainClassName) {
        this.mainClassName = mainClassName;
    }

    @SuppressWarnings("unchecked")
    @JsonProperty("OperatesOnColumns")
    public List<String> getOperatesOnColumns() {
        if (operatesOnColumns == null) {
            return new ArrayList<>();
        }
        return Arrays.asList(StringUtils.split(operatesOnColumns));
    }

    @JsonProperty("OperatesOnColumns")
    public void setOperatesOnColumns(List<String> operatesOnColumnsStr) {
        String[] s = new String[operatesOnColumnsStr.size()];
        operatesOnColumnsStr.toArray(s);
        this.operatesOnColumns = StringUtils.join(s);
    }

    public int getSortKey() {
        return sortKey;
    }

    public void setSortKey(int sortKey) {
        this.sortKey = sortKey;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @JsonProperty("NamedParameterListToInit")
    public Map<String, Object> getNamedParameterListToInit() {
        if (namedParameterListToInit == null) {
            return new HashMap<>();
        }
        return JsonUtils.deserialize(namedParameterListToInit, Map.class);
    }

    @JsonProperty("NamedParameterListToInit")
    public void setNamedParameterListToInit(Map<String, Object> namedParameterListToInit) {
        this.namedParameterListToInit = JsonUtils.serialize(namedParameterListToInit);
    }

}
