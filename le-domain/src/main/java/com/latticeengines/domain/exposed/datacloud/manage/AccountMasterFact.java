package com.latticeengines.domain.exposed.datacloud.manage;


import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "AccountMasterFact", uniqueConstraints = {@UniqueConstraint(columnNames = {
        "Location", //
        "Industry", //
        "NumEmpRange", //
        "RevRange", //
        "NumLocRange", //
        "Category" })
})
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccountMasterFact implements HasPid {

    public static final String DIM_LOCATION = "Location";
    public static final String DIM_INDUSTRY = "Industry";
    public static final String DIM_NUM_EMP_RANGE = "NumEmpRange";
    public static final String DIM_REV_RANGE = "RevRange";
    public static final String DIM_NUM_LOC_RANGE = "NumLocRange";
    public static final String DIM_CATEGORY = "Category";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Index(name = "IX_DIMENSIONS")
    @Column(name = DIM_LOCATION, nullable = false)
    private Long location;

    @Index(name = "IX_DIMENSIONS")
    @Column(name = DIM_INDUSTRY, nullable = false)
    private Long industry;

    @Index(name = "IX_DIMENSIONS")
    @Column(name = DIM_NUM_EMP_RANGE, nullable = false)
    private Long numEmpRange;

    @Index(name = "IX_DIMENSIONS")
    @Column(name = DIM_REV_RANGE, nullable = false)
    private Long revRange;

    @Index(name = "IX_DIMENSIONS")
    @Column(name = DIM_NUM_LOC_RANGE, nullable = false)
    private Long numLocRange;

    @Index(name = "IX_DIMENSIONS")
    @Column(name = DIM_CATEGORY, nullable = false)
    private Long category;

    @Lob
    @Column(name = "EncodedCube", nullable = false)
    private String encodedCube;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Long getLocation() {
        return location;
    }

    public void setLocation(Long location) {
        this.location = location;
    }

    public Long getIndustry() {
        return industry;
    }

    public void setIndustry(Long industry) {
        this.industry = industry;
    }

    public Long getNumEmpRange() {
        return numEmpRange;
    }

    public void setNumEmpRange(Long numEmpRange) {
        this.numEmpRange = numEmpRange;
    }

    public Long getRevRange() {
        return revRange;
    }

    public void setRevRange(Long revRange) {
        this.revRange = revRange;
    }

    public Long getNumLocRange() {
        return numLocRange;
    }

    public void setNumLocRange(Long numLocRange) {
        this.numLocRange = numLocRange;
    }

    public Long getCategory() {
        return category;
    }

    public void setCategory(Long category) {
        this.category = category;
    }

    public String getEncodedCube() {
        return encodedCube;
    }

    public void setEncodedCube(String encodedCube) {
        this.encodedCube = encodedCube;
    }
}
