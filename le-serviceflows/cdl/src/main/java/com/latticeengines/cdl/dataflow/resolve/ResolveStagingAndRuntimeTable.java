package com.latticeengines.cdl.dataflow.resolve;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema.Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.strategy.KVAttrPicker;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.flows.cdl.FieldLoadStrategy;
import com.latticeengines.domain.exposed.dataflow.flows.cdl.KeyLoadStrategy;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.ResolveStagingAndRuntimeTableParameters;

@Component("resolveStagingAndRuntimeTable")
public class ResolveStagingAndRuntimeTable
        extends TypesafeDataFlowBuilder<ResolveStagingAndRuntimeTableParameters> {

    @SuppressWarnings("unchecked")
    @Override
    public Node construct(ResolveStagingAndRuntimeTableParameters parameters) {
        FieldLoadStrategy fieldLoadStrategy = getFieldLoadStrategy(parameters);
        KeyLoadStrategy keyLoadStrategy = getKeyLoadStrategy(parameters);
        String stageTableName = parameters.stageTableName;
        String runtimeTableName = parameters.runtimeTableName;

        DataFlowContext ctx = getDataFlowCtx();
        Map<String, Table> sourceTables = ctx.getProperty(DataFlowProperty.SOURCETABLES, Map.class);
        Table stageTable = sourceTables.get(stageTableName);
        Table runtimeTable = sourceTables.get(runtimeTableName);

        if (keyLoadStrategy == KeyLoadStrategy.Full || runtimeTable == null) {
            return addSource(stageTableName);
        }

        Node extract = addSource(stageTableName);
        Node runtime = addSource(runtimeTableName);

        List<String> idColumns = stageTable.getPrimaryKey().getAttributes();

        extract = extract.kvDepivot(new FieldList(idColumns), new FieldList(idColumns));
        runtime = runtime.kvDepivot(new FieldList(idColumns), new FieldList(idColumns));

        extract = extract.addColumnWithFixedValue("TableType", "Extract", String.class);
        runtime = runtime.addColumnWithFixedValue("TableType", "Runtime", String.class);

        Node kv = extract.merge(runtime);
        List<Triple<Node, Class<?>, Set<String>>> nodes = getFilteredAttributes(kv, stageTable, runtimeTable);

        List<Node> picked = new ArrayList<>();
        Node firstPicked = null;
        int i = 0;
        Iterator<Triple<Node, Class<?>, Set<String>>> it = nodes.iterator();

        Map<String, FieldMetadata> fm = new HashMap<>();
        while (it.hasNext()) {
            Triple<Node, Class<?>, Set<String>> t = it.next();
            Node n = t.getLeft().kvPickAttr(idColumns.get(0), new AttributePicker(t.getMiddle(), fieldLoadStrategy))
                    .discard("TableType");
            if (i == 0) {
                firstPicked = n;
            } else {
                picked.add(n);
            }

            for (String name : t.getRight()) {
                if (name.equals(idColumns.get(0))) {
                    continue;
                }
                if (!fm.containsKey(name)) {
                    fm.put(name, new FieldMetadata(name, t.getMiddle()));
                }

            }
            i++;
        }

        kv = firstPicked.merge(picked);
        return kv.kvReconstruct(idColumns.get(0), new ArrayList<>(fm.values()));
    }

    private List<Triple<Node, Class<?>, Set<String>>> getFilteredAttributes(Node kv, Table stageTable,
            Table runtimeTable) {
        List<Triple<Node, Class<?>, Set<String>>> filteredAttributes = new ArrayList<>();

        Map<Class<?>, Set<String>> allFieldsPerType = getTypeToFieldNamesMap(stageTable);
        Map<Class<?>, Set<String>> runtimeFieldsPerType = getTypeToFieldNamesMap(runtimeTable);

        for (Map.Entry<Class<?>, Set<String>> entry : runtimeFieldsPerType.entrySet()) {
            Class<?> key = entry.getKey();
            if (allFieldsPerType.containsKey(key)) {
                Set<String> fields = allFieldsPerType.get(key);
                fields.addAll(entry.getValue());
                allFieldsPerType.put(key, fields);
            }
        }

        int i = 1;
        for (Map.Entry<Class<?>, Set<String>> entry : allFieldsPerType.entrySet()) {
            String expression = getJavaExpression(entry.getValue());
            Node node = kv.filter(expression, new FieldList(KVDepivotStrategy.KEY_ATTR)).renamePipe("attr" + i);
            Triple<Node, Class<?>, Set<String>> triple = new ImmutableTriple<>(node, entry.getKey(), entry.getValue());
            filteredAttributes.add(triple);
            i++;
        }

        return filteredAttributes;
    }

    private String getJavaExpression(Set<String> fieldNames) {
        List<String> expr = new ArrayList<>();

        for (String fieldName : fieldNames) {
            expr.add(String.format("%s.equals(\"%s\")", KVDepivotStrategy.KEY_ATTR, fieldName));
        }

        return StringUtils.join(expr, " || ");
    }

    private Map<Class<?>, Set<String>> getTypeToFieldNamesMap(Table table) {
        Map<Class<?>, Set<String>> map = new HashMap<>();

        for (Attribute field : table.getAttributes()) {
            String type = field.getPhysicalDataType();
            Class<?> javaType = AvroUtils.getJavaType(Type.valueOf(type.toUpperCase()));

            Set<String> fieldNames = map.get(javaType);

            if (fieldNames == null) {
                fieldNames = new HashSet<>();
            }
            fieldNames.add(field.getName());
            map.put(javaType, fieldNames);
        }
        return map;
    }

    private FieldLoadStrategy getFieldLoadStrategy(ResolveStagingAndRuntimeTableParameters parameters) {
        FieldLoadStrategy fieldLoadStrategy = parameters.fieldLoadStrategy;

        if (fieldLoadStrategy == null) {
            return FieldLoadStrategy.Update;
        }
        return fieldLoadStrategy;
    }

    private KeyLoadStrategy getKeyLoadStrategy(ResolveStagingAndRuntimeTableParameters parameters) {
        KeyLoadStrategy keyLoadStrategy = parameters.keyLoadStrategy;

        if (keyLoadStrategy == null) {
            return KeyLoadStrategy.Incremental;
        }
        return keyLoadStrategy;
    }

    private static class AttributePicker implements KVAttrPicker {
        private static final long serialVersionUID = 1L;

        private static final Collection<String> HELP_FIELDS = Collections.singleton("TableType");

        private String valClzName;

        private FieldLoadStrategy fieldLoadStrategy;

        public AttributePicker(Class<?> valClz, FieldLoadStrategy fieldLoadStrategy) {
            valClzName = valClz.getSimpleName();
            this.fieldLoadStrategy = fieldLoadStrategy;
        }

        @Override
        public Collection<String> helpFieldNames() {
            return HELP_FIELDS;
        }

        @Override
        public String valClzSimpleName() {
            return valClzName;
        }

        @Override
        public Object updateHelpAndReturnValue(Object oldValue, Map<String, Object> oldHelp, Object newValue,
                Map<String, Object> newHelp) {

            String oldSource = (String) oldHelp.get("TableType");
            String newSource = (String) newHelp.get("TableType");

            boolean updateWithNewValue = false;
            if (oldSource == null) {
                updateWithNewValue = true;
            } else if (newSource.equals("Extract")) {
                if (newValue != null) {
                    updateWithNewValue = true;
                } else if (fieldLoadStrategy == FieldLoadStrategy.Update) {
                    updateWithNewValue = false;
                }
            } else if (newSource.equals("Runtime")) {
                if (newValue != null && oldValue == null && fieldLoadStrategy == FieldLoadStrategy.Update) {
                    updateWithNewValue = true;
                }
            } else {
                updateWithNewValue = false;
            }
            if (updateWithNewValue) {
                oldHelp.put("TableType", newSource);
                return newValue;
            } else {
                return oldValue;
            }
        }
    }
}
