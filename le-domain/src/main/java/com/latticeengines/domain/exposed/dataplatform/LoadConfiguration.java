package com.latticeengines.domain.exposed.dataplatform;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.common.exposed.util.JsonUtils;

public class LoadConfiguration {

    private String table;
    private String metadataTable;
    private String customer;
    private DbCreds creds;
    private List<String> keyCols = new ArrayList<String>();
    
    public String getTable() {
        return table;
    }
    
    public void setTable(String table) {
        this.table = table;
    }

    @JsonProperty("metadata_table")
    public String getMetadataTable() {
        return metadataTable;
    }
    
    @JsonProperty("metadata_table")
    public void setMetadataTable(String metadataTable) {
        this.metadataTable = metadataTable;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public DbCreds getCreds() {
        return creds;
    }

    public void setCreds(DbCreds creds) {
        this.creds = creds;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonProperty("key_columns")
	public List<String> getKeyCols() {
		return keyCols;
	}

    @JsonProperty("key_columns")
	public void setKeyCols(List<String> keyCols) {
		this.keyCols = keyCols;
	}

}
