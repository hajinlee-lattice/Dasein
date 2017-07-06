package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class OrbCacheSeedRebuildBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -7247626960011947944L;

    protected Map<String, Integer> namePositionMap;

    private String companyFileDomainField;
    private String companyFileWebDomainsField;
    private String domainFileDomainField;
    private String domainFileHasEmailField;
    private int orbCacheSeedDomainLoc;
    private int orbCacheSeedPrimaryDomainLoc;
    private int orbCacheSeedIsSecondaryDomainLoc;
    private int orbCacheSeedDomainHasEmailLoc;

    private String primaryDomain;
    private Map<String, Boolean> domainHasEmailMap;

    private OrbCacheSeedRebuildBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public OrbCacheSeedRebuildBuffer(Fields fieldDeclaration, String companyFileDomainField,
            String companyFileWebDomainsField, String domainFileDomainField, String domainFileHasEmailField,
            String orbCacheSeedDomainField, String orbCacheSeedPrimaryDomainField,
            String orbCacheSeedIsSecondaryDomainField, String orbCacheSeedDomainHasEmailField) {
        this(fieldDeclaration);
        this.companyFileDomainField = companyFileDomainField;
        this.companyFileWebDomainsField = companyFileWebDomainsField;
        this.domainFileDomainField = domainFileDomainField;
        this.domainFileHasEmailField = domainFileHasEmailField;
        this.orbCacheSeedDomainLoc = namePositionMap.get(orbCacheSeedDomainField);
        this.orbCacheSeedPrimaryDomainLoc = namePositionMap.get(orbCacheSeedPrimaryDomainField);
        this.orbCacheSeedIsSecondaryDomainLoc = namePositionMap.get(orbCacheSeedIsSecondaryDomainField);
        this.orbCacheSeedDomainHasEmailLoc = namePositionMap.get(orbCacheSeedDomainHasEmailField);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        primaryDomain = null;
        domainHasEmailMap = new HashMap<>();
        TupleEntry group = bufferCall.getGroup();
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        setupTupleForArgument(bufferCall, arguments, group);
    }

    private void setupTupleForArgument(BufferCall bufferCall, Iterator<TupleEntry> argumentsInGroup, TupleEntry group) {
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            if (primaryDomain == null && StringUtils.isNotEmpty(arguments.getString(companyFileDomainField))) {
                primaryDomain = arguments.getString(companyFileDomainField);
                domainHasEmailMap.put(primaryDomain, null);
            }
            String companyFileWebDomain = arguments.getString(companyFileWebDomainsField);
            if (StringUtils.isNotEmpty(companyFileWebDomain)) {
                String[] companyFileWebDomains = companyFileWebDomain.split(",");
                for (String domain : companyFileWebDomains) {
                    if (StringUtils.isNotEmpty(domain)) {
                        domainHasEmailMap.put(domain, null);
                    }
                }
            }
            String domainFileDomain = arguments.getString(domainFileDomainField);
            if (StringUtils.isNotEmpty(domainFileDomain)) {
                Boolean domainFileHasEmail = arguments.getBoolean(domainFileHasEmailField);
                domainHasEmailMap.put(domainFileDomain, domainFileHasEmail);
            }
        }
        for (Map.Entry<String, Boolean> entry : domainHasEmailMap.entrySet()) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            setupTupleForGroup(result, group);
            result.set(orbCacheSeedDomainLoc, entry.getKey());
            result.set(orbCacheSeedPrimaryDomainLoc, primaryDomain);
            result.set(orbCacheSeedIsSecondaryDomainLoc,
                    primaryDomain.equals(entry.getKey()) ? Boolean.FALSE : Boolean.TRUE);
            result.set(orbCacheSeedDomainHasEmailLoc, entry.getValue());
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

    private void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        for (Object field : fields) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            }
        }
    }

}
