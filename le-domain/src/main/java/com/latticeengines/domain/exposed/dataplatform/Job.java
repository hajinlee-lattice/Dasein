package com.latticeengines.domain.exposed.dataplatform;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;

@Entity
@Table(name = "JOB", indexes = { @Index(name = "IX_JOB_ID", columnList = "JOB_ID") })
@Inheritance(strategy = InheritanceType.JOINED)
public class Job implements HasPid, HasId<String> {

    private static final Logger log = LoggerFactory.getLogger(Job.class);
    protected Long pid;
    protected String id;
    protected String client;
    protected String customer;
    protected Properties appMasterProperties = new Properties();
    protected Properties containerProperties = new Properties();

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "JOB_PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return this.pid;
    }

    @JsonIgnore
    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    @Column(name = "JOB_ID", nullable = false)
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @JsonIgnore
    @Transient
    public ApplicationId getAppId() {
        return ApplicationIdUtils.toApplicationIdObj(id);
    }

    @Column(name = "CUSTOMER")
    public String getCustomer() {
        return customer;
    }

    @Column(name = "CUSTOMER")
    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @JsonIgnore
    /**
     * string representation is ignored; json uses
     * getAppMasterPropertiesObject()
     **/
    @Column(name = "APPMASTER_PROPERTIES")
    @Lob
    public String getAppMasterProperties() {
        String propstr = "";
        try {
            StringWriter strWriter = new StringWriter();
            if (appMasterProperties != null && !appMasterProperties.isEmpty()) {
                appMasterProperties.store(strWriter, null);
            }
            propstr = strWriter.toString();
        } catch (IOException e) {
            log.error("Error serializing the properties value", e);
        }

        return propstr;
    }

    @JsonIgnore
    /**
     * string representation is ignored; json uses
     * setAppMasterPropertiesObject()
     **/
    /** data store has string / varchar as persistence type **/
    public void setAppMasterProperties(String appMasterPropertiesStr) throws IOException {
        this.appMasterProperties = new Properties();
        if (appMasterPropertiesStr != null && !appMasterPropertiesStr.equalsIgnoreCase("")) {
            this.appMasterProperties.load(new StringReader(appMasterPropertiesStr));
        }
    }

    @Transient
    public Properties getAppMasterPropertiesObject() {
        return appMasterProperties;
    }

    public void setAppMasterPropertiesObject(Properties appMasterPropertiesObject) {
        this.appMasterProperties = appMasterPropertiesObject;
        if (this.appMasterProperties == null) {
            this.appMasterProperties = new Properties();
        }
    }

    @JsonIgnore
    /**
     * string representation is ignored; json uses
     * getContainerPropertiesObject()
     **/
    @Column(name = "CONTAINER_PROPERTIES")
    @Lob
    public String getContainerProperties() {
        String propstr = "";
        try {
            StringWriter strWriter = new StringWriter();
            if (containerProperties != null && !containerProperties.isEmpty()) {
                containerProperties.store(strWriter, null);
            }
            propstr = strWriter.toString();
        } catch (IOException e) {
            log.error("Error serializing the properties value", e);
        }

        return propstr;
    }

    @JsonIgnore
    /**
     * string representation is ignored; json uses
     * setContainerPropertiesObject()
     **/
    public void setContainerProperties(String containerPropertiesStr) throws IOException {
        this.containerProperties = new Properties();
        if (containerPropertiesStr != null && !containerPropertiesStr.equalsIgnoreCase("")) {
            this.containerProperties.load(new StringReader(containerPropertiesStr));
        }
    }

    @Transient
    public Properties getContainerPropertiesObject() {
        return this.containerProperties;
    }

    public void setContainerPropertiesObject(Properties containerPropertiesObject) {
        this.containerProperties = containerPropertiesObject;
        if (this.containerProperties == null) {
            this.containerProperties = new Properties();
        }
    }

    /** data store has string / varchar as persistence type **/

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Column(name = "CLIENT", nullable = false)
    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    /**
     * http://docs.jboss.org/hibernate/core/4.0/manual/en-US/html/persistent-
     * classes.html#persistent-classes-equalshashcode
     */
    @Override
    public int hashCode() {
        int result;
        result = getId().hashCode();
        result = 29 * result + getClient().hashCode();
        return result;
    }

    /**
     * http://docs.jboss.org/hibernate/core/4.0/manual/en-US/html/persistent-
     * classes.html#persistent-classes-equalshashcode
     *
     * right now, it only perform a partially shallow comparison due to
     * efficiency reason. Collection object is compared, but composite domain
     * object is not compared. If composite domain object needs to be compared,
     * its equals() method has to be explicitly called.
     */
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

        Job job = (Job) obj;

        return new EqualsBuilder().append(pid, job.getPid()).append(id, job.getId())
                .append(client, job.getClient()) //
                .append(appMasterProperties, job.getAppMasterPropertiesObject()) //
                .append(containerProperties, job.getContainerPropertiesObject()).isEquals();

    }
}
