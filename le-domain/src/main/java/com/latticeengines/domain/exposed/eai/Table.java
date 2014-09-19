package com.latticeengines.domain.exposed.eai;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.HasName;

public class Table implements HasName {
    
    private String name;
    private List<Attribute> attributes = new ArrayList<>();

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }
    
    public void addAttribute(Attribute attribute) {
        attributes.add(attribute);
    }
    
    public List<Attribute> getAttributes() {
        return attributes;
    }
    

}
