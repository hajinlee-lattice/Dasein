package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AttrGroupCountBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -570985038993978462L;
    private static final Log log = LogFactory.getLog(AttrGroupCountBuffer.class);


    private static final String totalCountAttr = "GroupTotal";

    protected Map<String, Integer> namePositionMap;

    private String[] attrs;
    private String[] categories;

    private long[] attrCount;

    private float totalRecords;
    private String version;
    private String versionField;
    private String attrField;
    private String countField;
    private String categoryField;
    private String percentField;

    public AttrGroupCountBuffer(String[] attrs, String[] categories, Fields fieldDeclaration, String version, long totalRecords,
                                String versionField, String attrField, String categoryField, String countField, String percentField) {
        super(fieldDeclaration);
        this.attrs = attrs;
        this.categories = categories;
        this.version = version;
        this.totalRecords = (float)totalRecords;
        this.versionField = versionField;
        this.attrField = attrField;
        this.countField = countField;
        this.categoryField = categoryField;
        this.percentField = percentField;
        this.namePositionMap = getPositionMap(fieldDeclaration);
        attrCount = new long[attrs.length];
        for (int i = 0; i< attrs.length; i++) {
            attrCount[i] = 0;
        }
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Tuple result = Tuple.size(getFieldDeclaration().size());

        TupleEntry group = bufferCall.getGroup();
        setupTupleForGroup(result, group);
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        long totalCount = countAttrsForArgument(arguments);

        Integer attrLoc = namePositionMap.get(attrField);
        Integer countLoc = namePositionMap.get(countField);
        Integer categoryLoc = namePositionMap.get(categoryField);
        Integer percentLoc = namePositionMap.get(percentField);
        Integer versionLoc = namePositionMap.get(versionField);

        log.info("Version " + version + " " + versionLoc);

        for (int i = 0; i < attrs.length; i++) {
            long count = attrCount[i];
            if (count == 0) {
                continue;
            }
            Tuple attrResult = new Tuple(result);
            attrResult.set(versionLoc, version);
            attrResult.set(attrLoc, attrs[i]);
            attrResult.set(categoryLoc, categories[i]);
            attrResult.setLong(countLoc, count);
            attrResult.setFloat(percentLoc, (float)count / totalRecords);
            bufferCall.getOutputCollector().add(attrResult);
        }

        result.set(attrLoc, totalCountAttr);
        result.set(versionLoc, version);
        result.set(categoryLoc, totalCountAttr);
        result.setLong(countLoc, totalCount);
        result.setFloat(percentLoc, (float)totalCount / totalRecords);
        bufferCall.getOutputCollector().add(result);
    }

    private void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        for (Object field : fields) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            } else {
                log.error("Can not find field name=" + fieldName);
            }
        }
    }

    private long countAttrsForArgument(Iterator<TupleEntry> argumentsInGroup) {
        int totalCount = 0;
        int records = 0;
        while (argumentsInGroup.hasNext()) {
            if (records % 2056 == 0) {
                System.out.println("Pocessed " + records);
            }
            records++;
            TupleEntry arguments = argumentsInGroup.next();

            for (int i = 0; i < attrs.length; i++) {
                String attr = attrs[i];
                Object value = null;
                try {
                    value = arguments.getObject(attr);
                } catch (Exception e) {
                    continue;
                }
                if (value != null) {
                    attrCount[i]++;
                }
            }
            totalCount += 1;
        }
        return totalCount;
    }
}
