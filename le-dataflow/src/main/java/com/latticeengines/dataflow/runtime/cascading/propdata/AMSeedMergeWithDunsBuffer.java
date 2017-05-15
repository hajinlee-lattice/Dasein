package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
public class AMSeedMergeWithDunsBuffer extends BaseOperation implements Buffer {

    private static final Log log = LogFactory.getLog(AMSeedMergeWithDunsBuffer.class);

    private static final long serialVersionUID = -5435451652686146347L;

    public static final String LE = "LE";
    public static final String DNB = "DnB";

    private Map<String, Integer> namePositionMap;
    private Map<String, String> dnbToAms;

    private String dnbDunsCol;
    private String dnbDomainCol;
    private String leDomainCol;
    private int amsDomainLoc;
    private String dnbIsPrimaryDomainCol;
    private int amsIsPrimaryDomainLoc;
    private int amsDomainSourceLoc;
    private String leDomainSourceCol;

    // le domain -> le domain source
    private Map<String, String> leDomainToSource;
    // Duns -> Firmographic attributes (ams attribute -> dnb value)
    private Map<String, Map<String, Object>> dunsToDnBFirmAttrs;
    // Duns -> <dnb domain -> dnb isPrimaryDomain>
    private Map<String, Map<String, String>> dunsToDnBDomains;

    private AMSeedMergeWithDunsBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public AMSeedMergeWithDunsBuffer(Fields fieldDeclaration, Map<String, String> dnbToAms, String dnbDunsCol,
            String dnbDomainCol, String leDomainCol, String dnbIsPrimaryDomainCol,
            String amsIsPrimaryDomainCol, String amsDomainCol, String amsDomainSourceCol, String leDomainSourceCol) {
        this(fieldDeclaration);
        this.dnbToAms = dnbToAms;
        this.dnbDunsCol = dnbDunsCol;
        this.dnbDomainCol = dnbDomainCol;
        this.leDomainCol = leDomainCol;
        this.amsDomainLoc = namePositionMap.get(amsDomainCol);
        this.dnbIsPrimaryDomainCol = dnbIsPrimaryDomainCol;
        this.amsIsPrimaryDomainLoc = namePositionMap.get(amsIsPrimaryDomainCol);
        this.amsDomainSourceLoc = namePositionMap.get(amsDomainSourceCol);
        this.leDomainSourceCol = leDomainSourceCol;
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        leDomainToSource = new HashMap<>();
        dunsToDnBFirmAttrs = new HashMap<>();
        dunsToDnBDomains = new HashMap<>();
        TupleEntry group = bufferCall.getGroup();
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        setupTupleForArgument(bufferCall, arguments, group);
    }

    private void setupTupleForArgument(BufferCall bufferCall, Iterator<TupleEntry> argumentsInGroup, TupleEntry group) {
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            String leDomain = arguments.getString(leDomainCol);
            String domainSource = arguments.getString(leDomainSourceCol);
            if (StringUtils.isNotEmpty(leDomain)) {
                leDomainToSource.put(leDomain, domainSource);
            }
            String dnbDuns = arguments.getString(dnbDunsCol);
            if (StringUtils.isEmpty(dnbDuns)) {
                log.error("Catch empty duns with non-empty du duns");
            } else {
                if (!dunsToDnBFirmAttrs.containsKey(dnbDuns)) {
                    Map<String, Object> map = new HashMap<>();
                    for (Map.Entry<String, String> entry : dnbToAms.entrySet()) {
                        if (StringUtils.isNotEmpty(entry.getValue())) {
                            map.put(entry.getKey(), arguments.getObject(entry.getValue()));
                        }
                    }
                    dunsToDnBFirmAttrs.put(dnbDuns, map);
                }
                String dnbDomain = arguments.getString(dnbDomainCol);
                String dnbIsPrimaryDomain = arguments.getString(dnbIsPrimaryDomainCol);
                if (!dunsToDnBDomains.containsKey(dnbDuns)) {
                    dunsToDnBDomains.put(dnbDuns, new HashMap<>());
                }
                dunsToDnBDomains.get(dnbDuns).put(dnbDomain, dnbIsPrimaryDomain);
            }

        }
        // Duns -> Firmographic attributes (ams attribute -> dnb value)
        for (Map.Entry<String, Map<String, Object>> entry : dunsToDnBFirmAttrs.entrySet()) {
            for (Map.Entry<String, String> leDomEnt : leDomainToSource.entrySet()) { // leDomainToSource could be empty
                Tuple result = Tuple.size(getFieldDeclaration().size());
                // Copy firmographic attribute values from dnb
                for (String attr : dnbToAms.keySet()) {
                    result.set(namePositionMap.get(attr), entry.getValue().get(attr));
                }
                result.set(amsDomainSourceLoc, leDomEnt.getValue());
                result.set(amsDomainLoc, leDomEnt.getKey());
                result.set(amsIsPrimaryDomainLoc, "Y");
                bufferCall.getOutputCollector().add(result);
            }
            for (Map.Entry<String, String> dnbDomEnt : dunsToDnBDomains.get(entry.getKey()).entrySet()) {
                if (!leDomainToSource.containsKey(dnbDomEnt.getKey())
                        && !(dnbDomEnt.getKey() == null && leDomainToSource.size() > 0)) {
                    Tuple result = Tuple.size(getFieldDeclaration().size());
                    for (String attr : dnbToAms.keySet()) {
                        result.set(namePositionMap.get(attr), entry.getValue().get(attr));
                    }
                    result.set(amsDomainSourceLoc, DNB);
                    result.set(amsDomainLoc, dnbDomEnt.getKey());
                    if (leDomainToSource.size() > 0) {
                        result.set(amsIsPrimaryDomainLoc, "N");
                    } else {
                        result.set(amsIsPrimaryDomainLoc, dnbDomEnt.getValue());
                    }
                    bufferCall.getOutputCollector().add(result);
                }
            }
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
}