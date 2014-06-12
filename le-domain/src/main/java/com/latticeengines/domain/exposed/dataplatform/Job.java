package com.latticeengines.domain.exposed.dataplatform;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.codehaus.jackson.annotate.JsonIgnore;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringTokenUtils;
import com.latticeengines.common.exposed.util.YarnUtils;

@Entity
@Table(name = "JOB")
public class Job implements HasPid, HasId<String> {
    
    private Long pid;
    private String id;
    private String client;
    private Model model;
    private Properties appMasterProperties = new Properties();
    private Properties containerProperties = new Properties();
    private Long parentJobId;
    private List<String> childJobIds = new ArrayList<String>();

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return this.pid;
    }

    @JsonIgnore
    @Override
    public void setPid(Long id) {
        this.pid = id;
    }

    @Override
    @Column(name = "JOB_ID")
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
        return YarnUtils.getApplicationIdFromString(id);
    }

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE })
    @JoinColumn(name = "FK_MODEL_ID")
    public Model getModel() {
        return model;
    }

    @JsonIgnore
    public void setModel(Model model) {
        this.model = model;
    }

    @JsonIgnore
    @Column(name = "APPMASTER_PROPERTIES")
    public String getAppMasterProperties() {
        String propstr = (appMasterProperties == null || appMasterProperties.isEmpty()) ? "" : appMasterProperties
                .toString();
        return propstr;
    }
    
    @JsonIgnore
    /** data store has string / varchar as persistence type **/
    public void setAppMasterProperties(String appMasterPropertiesStr) throws IOException {
        this.appMasterProperties = new Properties();
        if (appMasterPropertiesStr != null && !appMasterPropertiesStr.equalsIgnoreCase(""))
            this.appMasterProperties.load(new StringReader(appMasterPropertiesStr));
    }
    
    @Transient
    public Properties getAppMasterPropertiesObject() {
        return appMasterProperties;
    }

    public void setAppMasterPropertiesObject(Properties appMasterPropertiesObject) {
        this.appMasterProperties = appMasterPropertiesObject;
        if (this.appMasterProperties == null)
            this.appMasterProperties = new Properties();
    }
      
    @JsonIgnore
    @Column(name = "CONTAINER_PROPERTIES")
    public String getContainerProperties() {
        String propstr = "";
        try {
            StringWriter strWriter = new StringWriter();
            if (containerProperties != null && !containerProperties.isEmpty())
                containerProperties.store(strWriter, null);
            propstr = strWriter.toString();
        } catch (IOException e) {
            // TODO log this message
            e.printStackTrace();
        }

        return propstr;
    }

    @JsonIgnore
    public void setContainerProperties(String containerPropertiesStr) throws IOException {
        this.containerProperties = new Properties();
        if (containerPropertiesStr != null && !containerPropertiesStr.equalsIgnoreCase(""))
            this.containerProperties.load(new StringReader(containerPropertiesStr));
    }

    @Transient
    public Properties getContainerPropertiesObject() {
        return this.containerProperties;
    }

    public void setContainerPropertiesObject(Properties containerPropertiesObject) {
        this.containerProperties = containerPropertiesObject;
        if (this.containerProperties == null)
            this.containerProperties = new Properties();
    }

    /** data store has string / varchar as persistence type **/

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Column(name = "CLIENT")
    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    @Column(name = "PARENT_JOB_ID")
    public Long getParentJobId() {
        return parentJobId;
    }

    public void setParentJobId(Long parentJobId) {
        this.parentJobId = parentJobId;
    }

    @Column(name = "CHILD_JOB_IDS")
    public String getChildJobIds() {
        // convert list to csv string
        return StringTokenUtils.listToString(this.childJobIds);
    }

    public void setChildJobIds(String listOfIds) {
        // convert csv string to list
        this.childJobIds = StringTokenUtils.stringToList(listOfIds);
    }

    public void setChildJobIdList(List<String> ids) {
        this.childJobIds = ids;
    }

    @Transient
    public List<String> getChildJobIdList() {
        return this.childJobIds;
    }

    public void addChildJobId(String jobId) {
        childJobIds.add(jobId);
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

        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!obj.getClass().equals(this.getClass()))
            return false;

        Job job = (Job) obj;

        return new EqualsBuilder().append(pid, job.getPid()).append(id, job.getId()).append(client, job.getClient())
                .append(model, job.getModel()).append(appMasterProperties, job.getAppMasterPropertiesObject())
                .append(containerProperties, job.getContainerPropertiesObject())
                .append(parentJobId, job.getParentJobId()).append(childJobIds, job.getChildJobIdList()).isEquals();
    }

}
