package com.latticeengines.transform.v2_0_25.functions;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public abstract class Lookup implements RealTimeTransform {

    private static final long serialVersionUID = -3604199243412683523L;

    static enum LookupType {
        StringToValue, //
        StringToList
    }

    protected Map<String, Object> lookupMap = new HashMap<>();

    public Lookup() {
    }

    public Lookup(String file, LookupType lookupType) {
        buildLookup(file, lookupType);
    }

    private void buildLookup(String file, LookupType lookupType) {
        try {
            String contents = FileUtils.readFileToString(new File(file));

            switch (lookupType) {
            case StringToValue:
                buildStringToValueLookup(contents);
                break;
            case StringToList:
                buildStringToListLookup(contents);
                break;
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot build lookup.", e);
        }
    }

    @SuppressWarnings("unchecked")
    void buildStringToValueLookup(String contents) {
        lookupMap = JsonUtils.deserialize(contents, Map.class, true);
    }

    void buildStringToListLookup(String contents) {
        contents = contents.replaceAll("u'", "'");
        contents = contents.replace('(', '[');
        contents = contents.replace(')', ']');
        buildStringToValueLookup(contents);
    }

    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        throw new UnsupportedOperationException();
    }

}