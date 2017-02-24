package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.db.PropertyBag;

public class ModelSummaryProvenance
        extends PropertyBag<ModelSummaryProvenanceProperty, ProvenancePropertyName> {

    @SuppressWarnings("unchecked")
    public ModelSummaryProvenance(
            List<ModelSummaryProvenanceProperty> modelSummaryProvenanceProperties) {
        super(List.class.cast(modelSummaryProvenanceProperties));
    }

    @SuppressWarnings("unchecked")
    public ModelSummaryProvenance() {
        super(List.class.cast(new ArrayList<ModelSummaryProvenanceProperty>()));
    }

    @JsonProperty
    public List<ModelSummaryProvenanceProperty> getBag() {
        return this.bag;
    }

    @JsonProperty
    public void setBag(List<ModelSummaryProvenanceProperty> bag) {
        this.bag = bag;
    }

    public void setModelSummary(ModelSummary modelSummary) {
        for (ModelSummaryProvenanceProperty provenanceProperty : this.getBag()) {
            provenanceProperty.setModelSummary(modelSummary);
        }
    }

    public void setProvenanceProperty(ProvenancePropertyName propertyName, Object value) {
        this.set(propertyName, value);
    }

    @JsonIgnore
    public String getProvenancePropertyString() {
        String ret = "";

        for (ProvenancePropertyName name : ProvenancePropertyName.values()) {
            String fmtString = " %s=%s";
            switch (name.getType().toString()) {
            case "class java.lang.Boolean":
                ret = ret.concat(
                        String.format(fmtString, name.getName(), this.getBoolean(name, false)));
                break;
            case "class java.lang.Integer":
                ret = ret.concat(String.format(fmtString, name.getName(), this.getInt(name, 0)));
                break;
            case "class java.lang.Double":
                ret = ret.concat(String.format(fmtString, name.getName(), this.getDouble(name, 0)));
            case "class java.lang.Long":
                ret = ret.concat(String.format(fmtString, name.getName(), this.getLong(name, 0)));
            default:
                ret = ret
                        .concat(String.format(fmtString, name.getName(), this.getString(name, "")));
                break;
            }
        }

        return ret;
    }
}
