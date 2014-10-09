package com.latticeengines.domain.exposed.jetty;

import java.util.Properties;

import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringTokenUtils;

public class JettyJobConfiguration {

    private String name;
    private String customer;
    private String jettyProperties;
    private String appMasterProperties;
    private String containerProperties;

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("customer")
    public String getCustomer() {
        return customer;
    }

    @JsonProperty("customer")
    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @JsonProperty("jetty_properties")
    public String getJettyProperties() {
        return jettyProperties;
    }

    @JsonProperty("jetty_properties")
    public void setJettyProperties(String jettyProperties) {
        this.jettyProperties = jettyProperties;
    }

    @JsonProperty("appmaster_properties")
    public String getAppMasterProperties() {
        return appMasterProperties;
    }

    @JsonProperty("appmaster_properties")
    public void setAppMasterProperties(String appMasterProperties) {
        this.appMasterProperties = appMasterProperties;
    }

    @JsonProperty("container_properties")
    public String getContainerProperties() {
        return containerProperties;
    }

    @JsonProperty("container_properties")
    public void setContainerProperties(String containerProperties) {
        this.containerProperties = containerProperties;
    }

    @JsonIgnore
    @Transient
    public Properties getJettyProps() {
        return StringTokenUtils.stringToProperty(getJettyProperties());
    }

    @JsonIgnore
    @Transient
    public Properties getAppMasterProps() {
        return StringTokenUtils.stringToProperty(getAppMasterProperties());
    }

    @JsonIgnore
    @Transient
    public Properties getContainerProps() {
        return StringTokenUtils.stringToProperty(getContainerProperties());
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
