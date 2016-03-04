package com.latticeengines.domain.exposed.propdata;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "MatchFramework_CommandParameter")
public class CommandParameter implements HasPid {

    public static final String KEY_INTERPRETED_DOMAIN = "InterpretedDomain";
    public static final String VALUE_YES = "Yes";

    @Id
    @Column(name = "MatchFramework_CommandParameter_ID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = { CascadeType.MERGE, CascadeType.REMOVE })
    @JoinColumn(name = "Command_ID", nullable = false)
    private Commands command;

    @Column(name = "Key_Name", nullable = false)
    private String key;

    @Column(name = "Value", nullable = false)
    private String value;

    @Column(name = "Is_Inferred", nullable = false)
    private Boolean inferred;

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

    @JsonIgnore
    public Commands getCommand() {
        return command;
    }

    @JsonIgnore
    public void setCommand(Commands command) {
        this.command = command;
    }

    @JsonProperty("Key")
    public String getKey() {
        return key;
    }

    @JsonProperty("Key")
    public void setKey(String key) {
        this.key = key;
    }

    @JsonProperty("Value")
    public String getValue() {
        return value;
    }

    @JsonProperty("Value")
    public void setValue(String value) {
        this.value = value;
    }

    @JsonProperty("Inferred")
    public Boolean getInferred() {
        return inferred;
    }

    @JsonProperty("Inferred")
    public void setInferred(Boolean inferred) {
        this.inferred = inferred;
    }
}
