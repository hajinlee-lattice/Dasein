package com.latticeengines.domain.exposed.modeling;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class DataSchema implements HasName {

    private String name;
    private String type;
    private String doc;
    private String tableName;
    private List<Field> fields = new ArrayList<Field>();
    
    public DataSchema() {
    }
    
    public DataSchema(Schema avroSchema) {
        List<org.apache.avro.Schema.Field> fields = avroSchema.getFields();
        
        for (org.apache.avro.Schema.Field field : fields) {
            Field f = new Field();
            f.setName(field.name());
            f.setColumnName(field.getProp("columnName"));
            f.setSqlType(Integer.parseInt(field.getProp("sqlType")));
            addField(f);
        }
    }

    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("type")
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("fields")
    public List<Field> getFields() {
        return fields;
    }

    public void addField(Field field) {
        fields.add(field);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonProperty("doc")
    public String getDoc() {
        return doc;
    }

    @JsonProperty("doc")
    public void setDoc(String doc) {
        this.doc = doc;
    }

    @JsonProperty("tableName")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty("tableName")
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

}
