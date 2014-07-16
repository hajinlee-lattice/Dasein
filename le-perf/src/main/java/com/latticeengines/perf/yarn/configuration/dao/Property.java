package com.latticeengines.perf.yarn.configuration.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "property")
@XmlAccessorType(XmlAccessType.FIELD)
public class Property {

    protected String name;

    protected String value;

    protected String source;

    public Property() {
    }

    public String getName() {
        return this.name;
    }

    public String getValue() {
        return this.value;
    }
}
