package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
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

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(AMSeedMergeWithDunsBuffer.class);

    private static final long serialVersionUID = -5435451652686146347L;

    public static final String LE = "LE";
    public static final String DNB = "DnB";

    private Map<String, Integer> namePositionMap;

    private String dnbDomCol;
    private String leDomCol;
    private int amsDomLoc;
    private String dnbIsPriDomCol;
    private int amsIsPriDomLoc;
    private int amsDomSrcLoc;
    private String leDomSrcCol;

    // le domain -> le domain source
    private Map<String, String> leDomToSrc;
    // dnb attr -> ams attr
    private Map<String, String> amsToDnB;
    // ams attr -> dnb firmographic attributes
    private Map<String, Object> amsToDnBFirmo;
    // dnb domain -> dnb isPrimaryDomain
    private Map<String, String> dnbDomToIsPri;

    private AMSeedMergeWithDunsBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public AMSeedMergeWithDunsBuffer(Fields fieldDeclaration, Map<String, String> amsToDnB, 
            String dnbDomCol, String leDomCol, String dnbIsPriDomCol, String amsIsPriDomCol, String amsDomCol,
            String amsDomainSourceCol, String leDomSrcCol) {
        this(fieldDeclaration);
        this.amsToDnB = amsToDnB;
        this.dnbDomCol = dnbDomCol;
        this.leDomCol = leDomCol;
        this.amsDomLoc = namePositionMap.get(amsDomCol);
        this.dnbIsPriDomCol = dnbIsPriDomCol;
        this.amsIsPriDomLoc = namePositionMap.get(amsIsPriDomCol);
        this.amsDomSrcLoc = namePositionMap.get(amsDomainSourceCol);
        this.leDomSrcCol = leDomSrcCol;
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        leDomToSrc = new HashMap<>();
        amsToDnBFirmo = new HashMap<>();
        dnbDomToIsPri = new HashMap<>();
        TupleEntry group = bufferCall.getGroup();
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        setupTupleForArgument(bufferCall, arguments, group);
    }

    private void setupTupleForArgument(BufferCall bufferCall, Iterator<TupleEntry> argumentsInGroup, TupleEntry group) {
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            String leDomain = arguments.getString(leDomCol);
            String domainSource = arguments.getString(leDomSrcCol);
            if (StringUtils.isNotBlank(leDomain)) {
                leDomToSrc.put(leDomain, domainSource);
            }
            if (amsToDnBFirmo.size() == 0) {
                for (Map.Entry<String, String> entry : amsToDnB.entrySet()) {
                    if (StringUtils.isNotEmpty(entry.getValue())) {
                        amsToDnBFirmo.put(entry.getKey(), arguments.getObject(entry.getValue()));
                    }
                }
            }
            String dnbDomain = arguments.getString(dnbDomCol);
            String dnbIsPrimaryDomain = arguments.getString(dnbIsPriDomCol);
            dnbDomToIsPri.put(dnbDomain, dnbIsPrimaryDomain);
        }
        // Domains from le
        for (Map.Entry<String, String> leDomEnt : leDomToSrc.entrySet()) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            // Copy firmographic attribute values from dnb
            for (String attr : amsToDnB.keySet()) {
                result.set(namePositionMap.get(attr), amsToDnBFirmo.get(attr));
            }
            result.set(amsDomSrcLoc, leDomEnt.getValue());
            result.set(amsDomLoc, leDomEnt.getKey());
            result.set(amsIsPriDomLoc, "Y");
            bufferCall.getOutputCollector().add(result);
        }
        // Clean up domains from DnB
        dnbDomToIsPri.entrySet().removeIf(dnbDomEnt -> leDomToSrc.containsKey(dnbDomEnt.getKey())
                || (StringUtils.isBlank(dnbDomEnt.getKey()) && (leDomToSrc.size() > 0 || dnbDomToIsPri.size() > 1)));
        // Domains from DnB
        for (Map.Entry<String, String> dnbDomEnt : dnbDomToIsPri.entrySet()) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            for (String attr : amsToDnB.keySet()) {
                result.set(namePositionMap.get(attr), amsToDnBFirmo.get(attr));
            }
            result.set(amsDomSrcLoc, DNB);
            result.set(amsDomLoc, dnbDomEnt.getKey());
            if (leDomToSrc.size() > 0 || StringUtils.isBlank(dnbDomEnt.getKey())) {
                result.set(amsIsPriDomLoc, "N");
            } else if (dnbDomToIsPri.size() == 1) {
                result.set(amsIsPriDomLoc, "Y");
            } else {
                result.set(amsIsPriDomLoc, dnbDomEnt.getValue());
            }
            bufferCall.getOutputCollector().add(result);
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