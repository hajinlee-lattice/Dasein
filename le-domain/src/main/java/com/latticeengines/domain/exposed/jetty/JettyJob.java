package com.latticeengines.domain.exposed.jetty;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.OneToMany;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;

import org.apache.commons.lang.builder.EqualsBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.Job;

@Entity
@Table(name = "JETTY_JOB")
@PrimaryKeyJoinColumn(name = "JOB_PID")
public class JettyJob extends Job implements HasName {

    private String name;
    private String customer;
    private List<JettyHost> jettyHosts = new ArrayList<>();

    @Override
    @Column(name = "NAME")
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;

    }

    @Column(name = "CUSTOMER")
    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @JsonIgnore
    @OneToMany(mappedBy = "jettyJob", targetEntity = JettyHost.class, fetch = FetchType.EAGER, cascade = { CascadeType.ALL })
    public List<JettyHost> getJettyHosts() {
        return jettyHosts;
    }

    @JsonIgnore
    public void setJettyHosts(List<JettyHost> jettyHosts) {
        this.jettyHosts = jettyHosts;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!obj.getClass().equals(this.getClass())) {
            return false;
        }

        JettyJob job = (JettyJob) obj;

        return new EqualsBuilder().append(pid, job.getPid()).append(id, job.getId()).append(client, job.getClient())//
                .append(appMasterProperties, job.getAppMasterPropertiesObject()).append(containerProperties, job.getContainerPropertiesObject())//
                .append(name, job.getName()).append(jettyHosts, job.getJettyHosts()).append(customer, job.getCustomer()).isEquals();
    }
}
