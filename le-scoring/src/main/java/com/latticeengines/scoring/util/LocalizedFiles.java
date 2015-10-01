package com.latticeengines.scoring.util;

import java.util.HashMap;

import org.json.simple.JSONObject;

public class LocalizedFiles {
    private JSONObject datatype;
    // key: modelGuid, value: model contents
    private HashMap<String, JSONObject> models;

    public JSONObject getDatatype() {
        return this.datatype;
    }

    public HashMap<String, JSONObject> getModels() {
        return this.models;
    }

    public void setModels(HashMap<String, JSONObject> models) {
        this.models = models;
    }

    public void setDatatype(JSONObject datatype) {
        this.datatype = datatype;
    }

}
