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

    private float totalRecords;
    private String version;
    private Integer attrLoc;
    private Integer countLoc;
    private Integer categoryLoc;
    private Integer percentLoc;
    private Integer versionLoc;

    private Map<Comparable, Integer> attrLocMap;


    public AttrGroupCountBuffer(String[] attrs, String[] categories, Fields fieldDeclaration, String version, long totalRecords,
                                String versionField, String attrField, String categoryField, String countField, String percentField) {
        super(fieldDeclaration);
        this.attrs = attrs;
        this.categories = categories;
        this.version = version;
        this.totalRecords = (float)totalRecords;
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.attrLoc = namePositionMap.get(attrField);
        this.countLoc = namePositionMap.get(countField);
        this.categoryLoc = namePositionMap.get(categoryField);
        this.percentLoc = namePositionMap.get(percentField);
        this.versionLoc = namePositionMap.get(versionField);
        attrLocMap = new HashMap<Comparable, Integer>();

        for (int i = 0; i < attrs.length; i++) {
             attrLocMap.put(attrs[i], i);
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
        long[] attrCount = new long[attrs.length];

        long totalCount = countAttrsForArgument(attrCount, arguments);

        for (int i = 0; i < attrs.length; i++) {
            long count = attrCount[i];
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

    private long countAttrsForArgument(long[] attrCount, Iterator<TupleEntry> argumentsInGroup) {
        int totalCount = 0;
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            Fields fields = arguments.getFields();
            int size = fields.size();

            for (int i = 0; i < size; i++) {
                if (arguments.getObject(i) != null) {
                    Integer attrLoc = attrLocMap.get(fields.get(i));
                    if (attrLoc != null) {
                        attrCount[attrLoc]++;
                    }
                }
            }
            totalCount += 1;
        }
        return totalCount;
    }
}
