package com.latticeengines.propdata.collection.dataflow.pivot;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class HGDataNewTechBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 6893329989555535052L;

    private static final String CATEGORY_1 = "HG_Category_1";
    private static final String LAST_VERIFIED = "Last_Verified_Date";
    private final String domainField;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

    HGDataNewTechBuffer(String domainField, String newTechField) {
        super(new Fields(domainField, newTechField));
        this.domainField = domainField;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        TupleEntry group = bufferCall.getGroup();
        String domain = group.getString(domainField);
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        String token = getNewTechsFromArguments(arguments);
        bufferCall.getOutputCollector().add(new Tuple(domain, token));
    }

    private String getNewTechsFromArguments(Iterator<TupleEntry> argumentsInGroup) {
        Map<String, Long> dateMap = new HashMap<>();
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            String cat1 = arguments.getString(CATEGORY_1);
            Long newDate = arguments.getLong(LAST_VERIFIED);
            if (dateMap.containsKey(cat1)) {
                Long foundDate = dateMap.get(cat1);
                if (foundDate >= newDate) {
                    dateMap.put(cat1, newDate);
                }
            } else {
                dateMap.put(cat1, newDate);
            }
        }

        List<String> tokens = new ArrayList<>();
        for (Map.Entry<String, Long> entry: dateMap.entrySet()) {
            String cat = entry.getKey();
            Long date = entry.getValue();
            tokens.add(cat + "(" + dateFormat.format(new Date(date)) + ")");
        }
        return StringUtils.join(tokens, ",");
    }


}
