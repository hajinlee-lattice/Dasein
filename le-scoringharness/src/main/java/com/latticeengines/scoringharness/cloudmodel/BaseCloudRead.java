package com.latticeengines.scoringharness.cloudmodel;

import java.util.ArrayList;

public class BaseCloudRead {
    public String objectType = null;
    public ArrayList<String> ids;
    public ArrayList<String> fields = null;

    public BaseCloudRead(String objectType, String id) {
        this.objectType = objectType;
        this.ids = new ArrayList<String>();
        this.fields = new ArrayList<String>();

        this.ids.add(id);
    }

    public void addId(String id) {
        ids.add(id);
    }
}
