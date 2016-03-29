package com.latticeengines.dataflow.exposed.builder.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FieldList {
    public static FieldList ALL = new FieldList(KindOfFields.ALL);
    public static FieldList RESULTS = new FieldList(KindOfFields.RESULTS);
    public static FieldList GROUP = new FieldList(KindOfFields.GROUP);
    public static FieldList NONE = new FieldList(KindOfFields.NONE);

    public enum KindOfFields {
        ALL, //
        RESULTS, //
        GROUP, //
        NONE
    }

    private List<String> fields;
    private KindOfFields kind;

    public FieldList(KindOfFields kind) {
        this.kind = kind;
    }

    public FieldList(String... fields) {
        this.fields = Arrays.asList(fields);
    }

    public FieldList(List<String> fields) {
        this.fields = new ArrayList<>(fields);
    }

    public String[] getFields() {
        if (fields == null) {
            return null;
        }
        return fields.toArray(new String[fields.size()]);
    }

    public List<String> getFieldsAsList() {
        if (fields == null) {
            return null;
        }
        return new ArrayList<>(fields);
    }

    public FieldList addAll(List<String> fields) {
        FieldList toReturn = new FieldList(this.fields);
        toReturn.kind = this.kind;
        toReturn.fields.addAll(fields);
        return toReturn;
    }

    public KindOfFields getKind() {
        return kind;
    }

}