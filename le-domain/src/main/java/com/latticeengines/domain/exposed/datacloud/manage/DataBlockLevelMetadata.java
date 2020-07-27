package com.latticeengines.domain.exposed.datacloud.manage;


import static com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevelMetadata.TABLE_NAME;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Access(AccessType.FIELD)
@Table(name = TABLE_NAME)
public class DataBlockLevelMetadata {

    public static final String TABLE_NAME = "DataBlockLevelMetadata";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "Block", nullable = false, length = 32)
    private String block;

    @Enumerated(EnumType.STRING)
    @Column(name = "Level", nullable = false, length = 4)
    private DataBlockLevel level;

    @Column(name = "Description", length = 1024)
    private String description;

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getBlock() {
        return block;
    }

    public void setBlock(String block) {
        this.block = block;
    }

    public DataBlockLevel getLevel() {
        return level;
    }

    public void setLevel(DataBlockLevel level) {
        this.level = level;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    // w/o version
    public String getFqBlockId() {
        return String.format("%s_%s", block, level);
    }
}
