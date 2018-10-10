package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasOptionAndValue;

@Entity
@Table(name = "TARGET_MARKET_DATA_FLOW_OPTION", //
        indexes = { @Index(name = "PROSPECT_DISCOVERY_OPTION_OPTION_IDX", columnList = "OPTION") })
public class TargetMarketDataFlowOption implements HasOptionAndValue, HasPid {

    private Long pid;
    private String option;
    private String value;
    private TargetMarket targetMarket;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @ManyToOne
    @JoinColumn(name = "TARGET_MARKET_ID", nullable = false)
    @JsonIgnore
    public TargetMarket getTargetMarket() {
        return targetMarket;
    }

    @JsonIgnore
    public void setTargetMarket(TargetMarket targetMarket) {
        this.targetMarket = targetMarket;
    }

    @Column(name = "OPTION", nullable = false)
    @JsonProperty("option")
    public String getOption() {
        return option;
    }

    public void setOption(String option) {
        this.option = option;
    }

    @Column(name = "VALUE", nullable = true)
    @JsonProperty("value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
