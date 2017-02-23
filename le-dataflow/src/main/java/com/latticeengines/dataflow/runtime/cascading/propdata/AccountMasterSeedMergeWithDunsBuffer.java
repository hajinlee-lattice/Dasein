package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
public class AccountMasterSeedMergeWithDunsBuffer extends BaseOperation implements Buffer {

    private static final Log log = LogFactory.getLog(AccountMasterSeedMergeWithDunsBuffer.class);

    private static final long serialVersionUID = -5435451652686146347L;

    public static final String LE = "LE";
    public static final String DNB = "DnB";

    private Map<String, Integer> namePositionMap;
    private Map<String, String> attrsFromDnB;

    private String dnbDunsColumn;
    private String dnbDomainColumn;
    private String leDomainColumn;
    private int amsDomainLoc;
    private String dnbIsPrimaryDomainColumn;
    private int amsIsPrimaryDomainLoc;
    private int amsDomainSourceLoc;

    // All the le domains for this group (DuDuns/Duns)
    private Set<String> domainsFromLe;
    // Duns -> Firmographic attributes (ams attribute -> dnb value)
    private Map<String, Map<String, Object>> firmAttrsFromDnB;
    // Duns -> <dnb domain -> dnb isPrimaryDomain>
    private Map<String, Map<String, String>> domainsFromDnB;

    private AccountMasterSeedMergeWithDunsBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public AccountMasterSeedMergeWithDunsBuffer(Fields fieldDeclaration, Map<String, String> attrsFromDnB,
            String dnbDunsColumn, String dnbDomainColumn, String leDomainColumn, String dnbIsPrimaryDomainColumn,
            String dnbDuDunsColumn, String amsIsPrimaryDomainColumn, String amsDomainColumn,
            String amsDomainSourceColumn) {
        this(fieldDeclaration);
        this.attrsFromDnB = attrsFromDnB;
        this.dnbDunsColumn = dnbDunsColumn;
        this.dnbDomainColumn = dnbDomainColumn;
        this.leDomainColumn = leDomainColumn;
        this.amsDomainLoc = namePositionMap.get(amsDomainColumn);
        this.dnbIsPrimaryDomainColumn = dnbIsPrimaryDomainColumn;
        this.amsIsPrimaryDomainLoc = namePositionMap.get(amsIsPrimaryDomainColumn);
        this.amsDomainSourceLoc = namePositionMap.get(amsDomainSourceColumn);
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        domainsFromLe = new HashSet<>();
        firmAttrsFromDnB = new HashMap<>();
        domainsFromDnB = new HashMap<>();
        TupleEntry group = bufferCall.getGroup();
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        setupTupleForArgument(bufferCall, arguments, group);
    }

    private void setupTupleForArgument(BufferCall bufferCall, Iterator<TupleEntry> argumentsInGroup, TupleEntry group) {
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            String leDomain = arguments.getString(leDomainColumn);
            if (StringUtils.isNotEmpty(leDomain)) {
                domainsFromLe.add(leDomain);
            }
            String dnbDuns = arguments.getString(dnbDunsColumn);
            if (StringUtils.isEmpty(dnbDuns)) {
                log.error("Catch empty duns with non-empty du duns");
            } else {
                if (!firmAttrsFromDnB.containsKey(dnbDuns)) {
                    Map<String, Object> map = new HashMap<>();
                    for (Map.Entry<String, String> entry : attrsFromDnB.entrySet()) {
                        if (StringUtils.isNotEmpty(entry.getValue())) {
                            map.put(entry.getKey(), arguments.getObject(entry.getValue()));
                        }
                    }
                    firmAttrsFromDnB.put(dnbDuns, map);
                }
                String dnbDomain = arguments.getString(dnbDomainColumn);// could be null  
                String dnbIsPrimaryDomain = arguments.getString(dnbIsPrimaryDomainColumn);
                if (!domainsFromDnB.containsKey(dnbDuns)) {
                    domainsFromDnB.put(dnbDuns, new HashMap<>());
                }
                domainsFromDnB.get(dnbDuns).put(dnbDomain, dnbIsPrimaryDomain);
            }

        }
        // Duns -> Firmographic attributes (ams attribute -> dnb value)
        for (Map.Entry<String, Map<String, Object>> entry : firmAttrsFromDnB.entrySet()) {
            for (String leDomain : domainsFromLe) { // could be empty
                Tuple result = Tuple.size(getFieldDeclaration().size());
                // Copy firmographic attribute values from dnb
                for (String attr : attrsFromDnB.keySet()) {
                    result.set(namePositionMap.get(attr), entry.getValue().get(attr));
                }
                result.set(amsDomainSourceLoc, LE);
                result.set(amsDomainLoc, leDomain);
                result.set(amsIsPrimaryDomainLoc, "Y");
                bufferCall.getOutputCollector().add(result);
            }
            for (Map.Entry<String, String> dnbDomainTuple : domainsFromDnB.get(entry.getKey()).entrySet()) {
                if (!domainsFromLe.contains(dnbDomainTuple.getKey())
                        && !(dnbDomainTuple.getKey() == null && domainsFromLe.size() > 0)) {
                    Tuple result = Tuple.size(getFieldDeclaration().size());
                    for (String attr : attrsFromDnB.keySet()) {
                        result.set(namePositionMap.get(attr), entry.getValue().get(attr));
                    }
                    result.set(amsDomainSourceLoc, DNB);
                    result.set(amsDomainLoc, dnbDomainTuple.getKey());
                    if (domainsFromLe.size() > 0) {
                        result.set(amsIsPrimaryDomainLoc, "N");
                    } else {
                        result.set(amsIsPrimaryDomainLoc, dnbDomainTuple.getValue());
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
