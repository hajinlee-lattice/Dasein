package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AttrGroupCountBuffer extends BaseOperation implements Buffer {


    private static final int MaxAttrs = 1024;
    private static final int MaxColSize = 4096;
    private static final int MaxAttrCountSize = 32;

    private static final char EqualOp = '@';
    private static final char Delimiter = '|';

    private static final long serialVersionUID = -570985038993978462L;
    private static final Logger log = LoggerFactory.getLogger(AttrGroupCountBuffer.class);


    protected Map<String, Integer> namePositionMap;

    private String version;
    private Integer totalLoc;
    private Integer[] attrLoc;
    private Integer versionLoc;

    private Map<Comparable, Integer> attrIdMap;
    private boolean[] validAttrIds;


    public AttrGroupCountBuffer(String[] attrs, Integer[] attrIds, Fields fieldDeclaration, String version,
                                String versionField, String[] attrField, String totalField) {
        super(fieldDeclaration);
        this.version = version;
        this.namePositionMap = getPositionMap(fieldDeclaration);

        this.versionLoc =  namePositionMap.get(versionField);
        this.attrLoc = new Integer[attrField.length];
        for (int i = 0; i < attrField.length; i++) {
            this.attrLoc[i] = namePositionMap.get(attrField[i]);
        }
        this.totalLoc = namePositionMap.get(totalField);

        attrIdMap = new HashMap<Comparable, Integer>();
        validAttrIds = new boolean[MaxAttrs];

        for (int i = 0; i < attrs.length; i++) {
             Integer attrId = attrIds[i];
             if (attrId >= MaxAttrs) {
                 log.info("Skip attribute " + attrs[i] + " for invalid id " + attrId);
                 continue;
             }
             attrIdMap.put(attrs[i], attrId);
             validAttrIds[attrId] = true;
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
        long[] attrCount = new long[MaxAttrs];

        long totalCount = countAttrsForArgument(attrCount, arguments);

        StringBuilder[] attrBuilder = new StringBuilder[attrLoc.length];
        int curAttrCol = 0;

        StringBuilder builder = new StringBuilder(MaxColSize);
        attrBuilder[curAttrCol] = builder;

        for (int i = 0; i < MaxAttrs; i++) {
            if (validAttrIds[i] == false) {
                continue;
            }
            long count = attrCount[i];
            builder.append(i);
            builder.append(EqualOp);
            builder.append(count);
            builder.append(Delimiter);
            if (builder.length() > (MaxColSize - MaxAttrCountSize)) {
                curAttrCol++;
                if (curAttrCol >=  attrLoc.length) {
                    log.info("Number of attributes exceeds the max buffer size");
                    break;
                }
                builder = new StringBuilder(MaxColSize);
                attrBuilder[curAttrCol] = builder;
            }
        }

        for (int i = 0; i < attrLoc.length; i++) {
            if (attrBuilder[i] == null) {
                break;
            }
            result.set(attrLoc[i], attrBuilder[i].toString());
        }
        result.setLong(totalLoc, totalCount);
        result.set(versionLoc, version);
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
                    Integer attrId = attrIdMap.get(fields.get(i));
                    if (attrId != null) {
                        attrCount[attrId]++;
                    }
                }
            }
            totalCount += 1;
        }
        return totalCount;
    }
}
