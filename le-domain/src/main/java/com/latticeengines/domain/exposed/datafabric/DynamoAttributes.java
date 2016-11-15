package com.latticeengines.domain.exposed.datafabric;

import java.util.ArrayList;
import java.util.List;

public class DynamoAttributes {

    private List<String> attributes = new ArrayList<>();
    
    public DynamoAttributes() {}
    
    public DynamoAttributes(List<String> attributes) {
        this.attributes = attributes;
    }
    
    public DynamoAttributes(String... attributes) {
        for (String attr : attributes) {
            this.attributes.add(attr);
        }
    }

    public List<String> getNames() {
        return attributes;
    }

}