package com.latticeengines.domain.exposed.eai;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;
import javax.persistence.Transient;

import com.latticeengines.common.exposed.util.StringTokenUtils;
import com.latticeengines.domain.exposed.dataplatform.Job;

@Entity
@Table(name = "EAI_JOB")
@PrimaryKeyJoinColumn(name = "JOB_PID")
public class EaiJob extends Job {

    private List<String> tables = new ArrayList<>();

    @Transient
    public List<String> getTableList() {
        return tables;
    }

    public void setTableList(List<String> tables) {
        this.tables = tables;
    }

    @Column(name = "TABLES")
    public String getTables() {
        return StringTokenUtils.listToString(tables);
    }

    @Column(name = "TABLES")
    public void setTables(String tablesString) {
        this.tables = StringTokenUtils.stringToList(tablesString);
    }
}
