package com.latticeengines.domain.exposed.ulysses;

import java.util.ArrayList;
import java.util.List;

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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@javax.persistence.Table(name = "ULYSSES_INSIGHT")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class Insight implements HasPid, HasName {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", unique = true, nullable = false)
    private String name;
    
    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE })
    @JoinColumn(name = "FK_CAMPAIGN_ID", nullable = false)
    private Campaign campaign;

    
    @JsonProperty("insight_sections")
    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "insight")
    private List<InsightSection> sections = new ArrayList<>();

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }


    public List<InsightSection> getSections() {
        return sections;
    }

    public void setSections(List<InsightSection> sections) {
        this.sections = sections;
    }

}
