package com.latticeengines.domain.exposed.jetty;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.apache.commons.lang.builder.EqualsBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "JETTY_HOST")
public class JettyHost implements HasPid {

    private Long pid;
    private JettyJob jettyJob;
    private String hostAddress;
    private int port;

    @Id
    @Override
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "JETTY_HOST_PID", unique = true, nullable = false)
    public Long getPid() {
        return this.pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "FK_JOB_PID", nullable = false)
    public JettyJob getJettyJob() {
        return jettyJob;
    }

    public void setJettyJob(JettyJob jettyJob) {
        this.jettyJob = jettyJob;
    }

    @Column(name = "HOST_ADDR", nullable = false)
    public String getHostAddress() {
        return hostAddress;
    }

    public void setHostAddress(String hostAddress) {
        this.hostAddress = hostAddress;
    }

    @Column(name = "PORT", nullable = false)
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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

        JettyHost jettyHost = (JettyHost) obj;

        return new EqualsBuilder().append(pid, jettyHost.getPid()).append(hostAddress, jettyHost.getHostAddress())
                .append(port, jettyHost.getPort()).isEquals();
    }
}
