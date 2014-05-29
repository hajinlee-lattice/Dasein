package com.latticeengines.domain.exposed.api;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "elements")
public class StringList {

    private List<String> elements = new ArrayList<String>();

    public StringList() {
    }

    public StringList(List<String> elements) {
        this.elements = elements;
    }

    @XmlElement(name = "element")
    public List<String> getElements() {
        return elements;
    }

}
