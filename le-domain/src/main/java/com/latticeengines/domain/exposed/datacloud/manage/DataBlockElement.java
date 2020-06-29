package com.latticeengines.domain.exposed.datacloud.manage;

import static com.latticeengines.domain.exposed.datacloud.manage.DataBlockElement.BLOCK;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlockElement.FQ_BLOCK_ID;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlockElement.ELEMENT;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlockElement.LEVEL;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlockElement.TABLE_NAME;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlockElement.VERSION;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = TABLE_NAME, //
        indexes = {
            @Index(name = "IX_DATA_BLOCK", columnList = FQ_BLOCK_ID),
            @Index(name = "IX_BLOCK_LEVEL", columnList = BLOCK + "," + LEVEL + "," + VERSION)
        }, //
        uniqueConstraints = { @UniqueConstraint(name = "UQ_DATA_BLOCK_ELEMENT", columnNames = {FQ_BLOCK_ID, ELEMENT }) })
public class DataBlockElement implements HasPid {

    public static final String TABLE_NAME = "DataBlockElement";
    public static final String FQ_BLOCK_ID = "BlockID";
    public static final String BLOCK = "Block";
    public static final String LEVEL = "Level";
    public static final String VERSION = "Version";
    public static final String ELEMENT = "Element";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    // fully qualified block id
    @Column(name = FQ_BLOCK_ID, nullable = false, length = 64)
    private String fqBlockId;

    @Column(name = BLOCK, nullable = false, length = 32)
    private String block;

    @Enumerated(EnumType.STRING)
    @Column(name = LEVEL, nullable = false, length = 4)
    private DataBlockLevel level;

    @Column(name = VERSION, nullable = false, length = 4)
    private String version;

    @ManyToOne
    @JoinColumn(name = ELEMENT, nullable = false)
    private PrimeColumn primeColumn;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getFqBlockId() {
        return fqBlockId;
    }

    public void setFqBlockId(String fqBlockId) {
        this.fqBlockId = fqBlockId;
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

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public PrimeColumn getPrimeColumn() {
        return primeColumn;
    }

    public void setPrimeColumn(PrimeColumn primeColumn) {
        this.primeColumn = primeColumn;
    }
}
