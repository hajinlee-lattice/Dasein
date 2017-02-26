package com.latticeengines.dataflow.exposed.builder.strategy.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.strategy.DepivotStrategy;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class KVDepivotStrategy implements DepivotStrategy {

    private static final long serialVersionUID = -1L;

    public static final String KEY_ATTR = "_KVOp_Key_";
    private static final String VALUE_ATTR_PREFIX = "_KVOp_Val_";

    public static final Set<Class<?>> SUPPORTED_CLASSES = new HashSet<>(Arrays.asList( //
            String.class, //
            Integer.class, //
            Long.class, //
            Float.class, //
            Double.class, //
            Boolean.class) //
    );

    private static final Map<String, Class<?>> RESERVED_FIELDS = new HashMap<>();

    static {
        RESERVED_FIELDS.put(KEY_ATTR, String.class);
        SUPPORTED_CLASSES.forEach(clz -> RESERVED_FIELDS.put(valueAttr(clz), clz));
    }

    private final Set<String> fieldsToAppend;
    private final Set<String> fieldsToSkip;
    private final Map<String, Integer> namePositionMap;
    private final Map<String, String> argTypeMap;
    private final Integer keyPos;
    private final Fields fieldDeclaration;

    public KVDepivotStrategy(Map<String, String> argTypeMap, FieldList fieldsToAppend, FieldList fieldsNotToPivot) {
        this.fieldDeclaration = constructDeclaration(argTypeMap, fieldsToAppend);
        this.fieldsToAppend = new HashSet<>(fieldsToAppend.getFieldsAsList());
        this.namePositionMap = getPositionMap();
        this.keyPos = this.namePositionMap.get(KEY_ATTR);
        if (fieldsNotToPivot == null) {
            this.fieldsToSkip = Collections.emptySet();
        } else {
            this.fieldsToSkip = new HashSet<>(Arrays.asList(fieldsNotToPivot.getFields()));
        }
        this.argTypeMap = argTypeMap;
    }

    @Override
    public Iterable<Tuple> depivot(TupleEntry arguments) {
        Fields fields = arguments.getFields();

        Map<String, Object> toAppend = new HashMap<>();
        List<Tuple> tuples = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            String field = (String) fields.get(i);
            Object value = arguments.getObject(i);
            if (!fieldsToSkip.contains(field)) {
                tuples.add(extractKV(field, value));
            }
            if (fieldsToAppend.contains(field)) {
                toAppend.put(field, value);
            }
        }

        for (Tuple tuple: tuples) {
            append(tuple, toAppend);
        }

        return tuples;
    }

    private Map<String, Integer> getPositionMap() {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

    private Tuple extractKV(String field, Object value) {
        String fieldClz = argTypeMap.get(field);
        String valueAttr = valueAttr(fieldClz);
        Integer idx = namePositionMap.get(valueAttr);

        Tuple tuple = Tuple.size(fieldDeclaration.size());
        tuple.set(keyPos, field);
        tuple.set(idx, value);

        return tuple;
    }

    private void append(Tuple tuple, Map<String, Object> toAppend) {
        for (Map.Entry<String, Object> entry: toAppend.entrySet()) {
            tuple.set(namePositionMap.get(entry.getKey()), entry.getValue());
        }
    }

    public static Fields constructDeclaration(Map<String, String> argTypeMap, FieldList fieldsToAppend) {
        List<String> fields = new ArrayList<>();
        if (fieldsToAppend != null) {
            fields.addAll(fieldsToAppend.getFieldsAsList());
        }
        fields.add(KEY_ATTR);
        Set<String> involvedClzSet = new HashSet<>();
        argTypeMap.values().forEach(clzName -> involvedClzSet.add(valueAttr(clzName)));
        List<String> involvedClz = new ArrayList<>(involvedClzSet);
        involvedClz.sort(String::compareTo);
        fields.addAll(involvedClz);
        return new Fields(fields.toArray(new String[fields.size()]));
    }

    public static String valueAttr(Class<?> attrClz) {
        return valueAttr(attrClz.getSimpleName());
    }

    public static String valueAttr(String attrClz) {
        return String.format("%s%s_", VALUE_ATTR_PREFIX, attrClz);
    }

    public static Class<?> reservedAttrType(String field) {
        if (RESERVED_FIELDS.containsKey(field)) {
            return RESERVED_FIELDS.get(field);
        } else {
            throw new IllegalArgumentException(field + " is not one of the reserved field name.");
        }
    }
}
