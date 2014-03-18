package com.latticeengines.dataplatform.exposed.domain;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.dataplatform.util.JsonHelper;

public class DataSchema implements HasName {

    private String name;
    private String type;
    private List<Field> fields = new ArrayList<Field>();
    
    public DataSchema() {
    }
    
    public DataSchema(Schema avroSchema) {
        List<org.apache.avro.Schema.Field> fields = avroSchema.getFields();
        
        for (org.apache.avro.Schema.Field field : fields) {
            Field f = new Field();
            f.setName(field.name());
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
        return JsonHelper.serialize(this);
    }

}
