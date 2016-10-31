package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "CountryCode", uniqueConstraints = { @UniqueConstraint(columnNames = { "CountryName" }) })
@JsonIgnoreProperties(ignoreUnknown = true)
public class CountryCode implements HasPid, Serializable {

    private static final long serialVersionUID = -8815334549574860144L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "CountryName", nullable = false)
    private String countryName;

    @Column(name = "ISOCountryName", nullable = false)
    private String isoCountryName;

    @Column(name = "ISOCountryCode2Char", nullable = false)
    private String isoCountryCode2Char;

    @Override
    @JsonIgnore
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @JsonProperty("CountryName")
    public String getCountryName() {
        return countryName;
    }

    @JsonProperty("CountryName")
    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }

    @JsonProperty("ISOCountryName")
    public String getIsoCountryName() {
        return isoCountryName;
    }

    @JsonProperty("ISOCountryName")
    public void setIsoCountryName(String isoCountryName) {
        this.isoCountryName = isoCountryName;
    }

    @JsonProperty("ISOCountryCode2Char")
    public String getIsoCountryCode2Char() {
        return isoCountryCode2Char;
    }

    @JsonProperty("ISOCountryCode2Char")
    public void setIsoCountryCode2Char(String isoCountryCode2Char) {
        this.isoCountryCode2Char = isoCountryCode2Char;
    }

}
