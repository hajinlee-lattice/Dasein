package com.latticeengines.dataflow.runtime.cascading.propdata.patch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DomainPatchBookBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = -7722652185793149359L;

    private String domainSource;
    private String patchDomainField;
    private int isPriLocIdx;
    private int isPriDomIdx;
    private int domSrcIdx;
    private int domIdx;

    // domain -> tuple, domain could be null (duns-only entry)
    private Map<String, TupleEntry> domainToTuple;
    // domains in domainsToRemove are guaranteed to be non-empty String in
    // DomainPatchBookAggregator, but the list could be empty
    private List<String> domainsToRemove;
    // domains in domainsToAdd are guaranteed to be non-empty String in
    // DomainPatchBookAggregator, but the list could be empty
    private List<String> domainsToAdd;
    // whether domainsToRemove and domainsToAdd have been initialized
    private boolean patchDomainsInit;

    public DomainPatchBookBuffer(Fields fieldDeclaration, String domainSource,
            String patchDomainField) {
        super(fieldDeclaration);
        Map<String, Integer> namePositionMap = getPositionMap(fieldDeclaration);
        this.domainSource = domainSource;
        this.patchDomainField = patchDomainField;
        isPriLocIdx = namePositionMap.get(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION);
        isPriDomIdx = namePositionMap.get(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN);
        domSrcIdx = namePositionMap.get(DataCloudConstants.AMS_ATTR_DOMAIN_SOURCE);
        domIdx = namePositionMap.get(DataCloudConstants.AMS_ATTR_DOMAIN);
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        // These class members need to re-initialized for each group, set them
        // as class member instead of local variable to avoid large method
        // signature
        domainToTuple = new HashMap<>();
        domainsToRemove = new ArrayList<>();
        domainsToAdd = new ArrayList<>();
        patchDomainsInit = false;
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        setup(arguments);
        patch();
        output(bufferCall);
    }

    /**
     * Bootstrap domainToTuple, domainsToRemove, domainsToAdd
     */
    @SuppressWarnings("unchecked")
    private void setup(Iterator<TupleEntry> arguments) {
        while (arguments.hasNext()) {
            TupleEntry argument = arguments.next();
            // argument is an unmodifiable tuple, need to create a modifiable
            // tuple entry
            argument = new TupleEntry(argument.getFields(), argument.getTupleCopy(), false);
            domainToTuple.put(argument.getString(DataCloudConstants.AMS_ATTR_DOMAIN), argument);
            // patchDomains is same for all the rows in the buffer, so only
            // initialize domainsToRemove and domainsToAdd once
            if (patchDomainsInit) {
                continue;
            }
            // Initialize domainsToRemove and domainsToAdd
            patchDomainsInit = true;
            String patchDomainStr = argument.getString(patchDomainField);
            // No domain to patch for this duns
            if (StringUtils.isBlank(patchDomainStr)) {
                continue;
            }
            Map map = JsonUtils.deserialize(patchDomainStr, Map.class);
            Map<String, List<String>> patchDomains = JsonUtils.convertMapWithListValue(map,
                    String.class, String.class);
            // No domain to cleanup for this duns
            if (CollectionUtils.isNotEmpty(patchDomains.get(DomainPatchBookAggregator.CLEANUP))) {
                domainsToRemove.addAll(patchDomains.get(DomainPatchBookAggregator.CLEANUP));
            }
            // No domain to add for this duns
            if (CollectionUtils.isNotEmpty(patchDomains.get(DomainPatchBookAggregator.ADD))) {
                domainsToAdd.addAll(patchDomains.get(DomainPatchBookAggregator.ADD));
            }
        }
    }

    private void patch() {
        add();
        cleanup();
    }

    private void add() {
        domainsToAdd.forEach(domain -> {
            // domain already exists, ignore
            if (domainToTuple.containsKey(domain)) {
                return;
            }
            // There is duns-only entry, populate domain for it, set
            // LE_IS_PRIMARY_LOCATION = N,
            // LE_IS_PRIMARY_DOMAIN = N, DomainSource = Patch, don't change
            // other field values
            if (domainToTuple.containsKey(null)) {
                TupleEntry tuple = domainToTuple.get(null);
                updateTuple(tuple, Boolean.FALSE, Boolean.FALSE, domain, domainSource);
                domainToTuple.remove(null);
                domainToTuple.put(domain, tuple);
                return;
            }
            // Add a row with domain to add, LE_IS_PRIMARY_LOCATION = N,
            // LE_IS_PRIMARY_DOMAIN = N, DomainSource = Patch, other field
            // values are same for all the records with same duns, so copy from
            // any record
            TupleEntry copyFrom = domainToTuple.values().iterator().next();
            TupleEntry tuple = new TupleEntry(copyFrom); // create a safe copy
            updateTuple(tuple, Boolean.FALSE, Boolean.FALSE, domain, domainSource);
            domainToTuple.put(domain, tuple);
        });
    }

    private void cleanup() {
        domainsToRemove.forEach(domain -> {
            // domain doesn't exist, ignore
            if (!domainToTuple.containsKey(domain)) {
                return;
            }
            // If there is only one row for this duns, set Domain = null,
            // DomainSource = null, LE_IS_PRIMARY_LOCATION = N,
            // LE_IS_PRIMARY_DOMAIN = N, don't change other field values
            if (domainToTuple.size() == 1) {
                TupleEntry tuple = domainToTuple.get(domain);
                updateTuple(tuple, Boolean.FALSE, Boolean.FALSE, null, null);
                domainToTuple.remove(domain);
                domainToTuple.put(null, tuple);
                return;
            }
            // If there are multiple rows for this duns, remove the row
            domainToTuple.remove(domain);
        });
    }

    private void updateTuple(TupleEntry tuple, Boolean isPriLoc, Boolean isPriDom, String domain,
            String domainSource) {
        if (isPriLoc != null) { // if null, keep original value
            tuple.getTuple().set(isPriLocIdx, Boolean.TRUE.equals(isPriLoc)
                    ? DataCloudConstants.ATTR_VAL_Y : DataCloudConstants.ATTR_VAL_N);
        }
        if (isPriDom != null) { // if null, keep original value
            tuple.getTuple().set(isPriDomIdx, Boolean.TRUE.equals(isPriDom)
                    ? DataCloudConstants.ATTR_VAL_Y : DataCloudConstants.ATTR_VAL_N);
        }
        tuple.getTuple().set(domSrcIdx, domainSource);
        tuple.getTuple().set(domIdx, domain);
    }

    private void output(BufferCall bufferCall) {
        domainToTuple.values().forEach(tuple -> {
            bufferCall.getOutputCollector().add(tuple);
        });
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
