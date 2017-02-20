package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Iterator;

import org.apache.commons.lang.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings({"unchecked", "rawtypes"})
public class FillPrimaryDomainBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 1L;

    private static final String LE_IS_PRIMARY_DOMAIN = "LE_IS_PRIMARY_DOMAIN";
    private static final String FLAG_DROP_LESS_POPULAR_DOMAIN = "_FLAG_DROP_LESS_POPULAR_DOMAIN_";
    private static final String DOMAIN = "Domain";

    // output all input fields
    public FillPrimaryDomainBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        TupleEntry group = bufferCall.getGroup();
        String duns = group.getString(0);
        if (StringUtils.isBlank(duns)) {
            // DUNS == null, nothing to do
            returnTuplesAsIs(bufferCall);
        } else {
            fillPrimaryDomain(bufferCall);
        }
    }

    private void fillPrimaryDomain(BufferCall bufferCall) {
        String primaryDomain = null;
        Iterator<TupleEntry> argumentsIter = bufferCall.getArgumentsIterator();
        while (argumentsIter.hasNext()) {
            TupleEntry arguments = argumentsIter.next();
            if (StringUtils.isBlank(primaryDomain)) {
                primaryDomain = arguments.getString(AccountMasterSeedDomainRankBuffer.MIN_RANK_DOMAIN);
            }
            String domain = arguments.getString(DOMAIN);
            if (domain.equalsIgnoreCase(primaryDomain)) {
                bufferCall.getOutputCollector().add(arguments);
            } else {
                TupleEntry tupleEntry = new TupleEntry(arguments);
                tupleEntry.setString(LE_IS_PRIMARY_DOMAIN, "N");
                tupleEntry.setString(FLAG_DROP_LESS_POPULAR_DOMAIN, primaryDomain);
                bufferCall.getOutputCollector().add(tupleEntry);
            }
        }
    }

    private void returnTuplesAsIs(BufferCall bufferCall) {
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext()) {
            bufferCall.getOutputCollector().add(iter.next());
        }
    }
}
