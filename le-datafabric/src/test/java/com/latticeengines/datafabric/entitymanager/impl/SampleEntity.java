package com.latticeengines.datafabric.entitymanager.impl;

import com.latticeengines.datafabric.util.RedisIndex;

import com.latticeengines.domain.exposed.dataplatform.HasId;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class SampleEntity implements HasId<String> {

    @Id
    private String id;

    private String name;

    @RedisIndex(name = "latticeId")
    private String latticeId;

    private String company;

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    String getLatticeId() {
        return latticeId;
    }

    public void setLatticeId(String latticeId) {
        this.latticeId = latticeId;
    }
}
