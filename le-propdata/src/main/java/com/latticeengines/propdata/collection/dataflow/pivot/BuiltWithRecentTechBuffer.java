package com.latticeengines.propdata.collection.dataflow.pivot;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import liquibase.util.StringUtils;

@SuppressWarnings("rawtypes")
public class BuiltWithRecentTechBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -8454562626526879123L;
    private static final String TECHNOLOGY_TAG = "Technology_Tag";
    private static final String TECHNOLOGY_NAME = "Technology_Name";
    private static final String TECHNOLOGY_FIRST = "Technology_First_Detected";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

    private static final Integer RECENT_TECH_LOC = 1;
    private static final Integer RECENT_TAG_LOC = 2;
    private static final int NUM_TAG = 15;

    private static Set<String> tags = new HashSet<>(Arrays.asList(
            "Web Server",
            "Analytics",
            "Feeds",
            "CDN",
            "Widgets",
            "CMS",
            "Server",
            "MX",
            "SSL",
            "Hosting",
            "Media",
            "Ads",
            "Payment",
            "Shop"));

    protected Map<String, Integer> namePositionMap;

    public BuiltWithRecentTechBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field: fieldDeclaration) {
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
        setupTupleForArgument(result, arguments);
        bufferCall.getOutputCollector().add(result);
    }

    private void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        for (Object field: fields) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            } else {
                System.out.println("Warning: can not find field name=" + fieldName);
            }
        }
    }

    private void setupTupleForArgument(Tuple result, Iterator<TupleEntry> argumentsInGroup) {
        Set<String> recentTechs = new HashSet<>();
        Set<String> recentTags = new HashSet<>();

        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            String tag = arguments.getString(TECHNOLOGY_TAG);
            String tech = arguments.getString(TECHNOLOGY_NAME);
            Long firstDetected = arguments.getLong(TECHNOLOGY_FIRST);

            if (tags.contains(tag)) {
                String techToken = tech + "(" + dateFormat.format(new Date(firstDetected)) + ")";
                String tagToken = tag + "(" + dateFormat.format(new Date(firstDetected)) + ")";
                if (recentTechs.size() < NUM_TAG) {
                    recentTechs.add(techToken);
                }
                if (recentTags.size() < NUM_TAG) {
                    recentTags.add(tagToken);
                }
            }
        }

        result.set(RECENT_TECH_LOC, StringUtils.join(recentTechs, ","));
        result.set(RECENT_TAG_LOC, StringUtils.join(recentTags, ","));
    }


}
