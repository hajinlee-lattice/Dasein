package com.latticeengines.perf.yarn.configuration.dao;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "configuration")
@XmlAccessorType(XmlAccessType.FIELD)
public class YarnConfigurationSettings {

    protected List<Property> property;

    public YarnConfigurationSettings() {
    }

    public List<Property> getProperties() {
        return this.property;
    }
}
