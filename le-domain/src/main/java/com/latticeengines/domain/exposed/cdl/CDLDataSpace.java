package com.latticeengines.domain.exposed.cdl;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CDLDataSpace implements Serializable {

    private static final long serialVersionUID = -2292678716486572472L;

	@JsonProperty("ActiveVersion")
    private DataCollection.Version activeVersion;

    @JsonProperty("Entities")
    private Map<BusinessEntity, Map<BusinessEntity.DataStore, List<TableSpace>>> entities;

    @JsonProperty("Others")
    private Map<String, List<TableSpace>> others;


    public CDLDataSpace() {}

    public DataCollection.Version getActiveVersion() {
        return activeVersion;
    }

    public void setActiveVersion(DataCollection.Version activeVersion) {
        this.activeVersion = activeVersion;
    }

    

    public Map<BusinessEntity, Map<BusinessEntity.DataStore, List<TableSpace>>> getEntities() {
        return entities;
    }

    public void setEntities(Map<BusinessEntity, Map<BusinessEntity.DataStore, List<TableSpace>>> entities) {
        this.entities = entities;
    }

    public Map<String, List<TableSpace>> getOthers() {
        return others;
    }

    public void setOthers(Map<String, List<TableSpace>> others) {
        this.others = others;
    }



    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class TableSpace{

        @JsonProperty("TableRole")
        private TableRoleInCollection tableRole;

        @JsonProperty("Table")
        private List<String> tables;

        @JsonProperty("HdfsPath")
        private List<String> HdfsPath;

        @JsonProperty("Version")
        private DataCollection.Version version;

        public TableSpace() {}

        public TableRoleInCollection getTableRole() {
            return tableRole;
        }

        public void setTableRole(TableRoleInCollection tableRole) {
            this.tableRole = tableRole;
        }

        public List<String> getTables() {
            return tables;
        }

        public void setTables(List<String> tables) {
            this.tables = tables;
        }

        public List<String> getHdfsPath() {
            return HdfsPath;
        }

        public void setHdfsPath(List<String> hdfsPath) {
            HdfsPath = hdfsPath;
        }

        public DataCollection.Version getVersion() {
            return version;
        }

        public void setVersion(DataCollection.Version version) {
            this.version = version;
        }

    }

}
