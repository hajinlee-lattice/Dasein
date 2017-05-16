package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Iterator;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings({"unchecked", "rawtypes"})
public class FillBlankDomainBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 1L;

    private String domainField;
    private String isPriDomField;

    private int primaryDomainArgIdx = -1;
    private int domainArgIdx = -1;
    private int isPriDomArgIdx = -1;

    // output all input fields
    public FillBlankDomainBuffer(Fields fieldDeclaration, String domainField, String isPriDomField) {
        super(fieldDeclaration);
        this.domainField = domainField;
        this.isPriDomField = isPriDomField;
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

        TupleEntry group = bufferCall.getGroup();
        String groupValue = group.getString(0);
        if (StringUtils.isBlank(groupValue)) {
            // DU == null, nothing to do
            returnTuplesAsIs(bufferCall);
        } else {
            fillBlankDomains(bufferCall);
        }
    }

    private void fillBlankDomains(BufferCall bufferCall) {
        String enrichingDomain = null;
        Iterator<TupleEntry> argumentsIter = bufferCall.getArgumentsIterator();
        while (argumentsIter.hasNext()) {
            TupleEntry arguments = argumentsIter.next();
            setArgPosMap(arguments);

            if (StringUtils.isBlank(enrichingDomain)) {
                enrichingDomain = getStringAt(arguments, primaryDomainArgIdx);
            }

            TupleEntry tupleEntry = new TupleEntry(arguments);
            String domain = getStringAt(arguments, domainArgIdx);
            if (StringUtils.isBlank(domain)) {
                tupleEntry.setString(domainField, enrichingDomain);
                tupleEntry.setString(isPriDomField, "Y");
            }

            bufferCall.getOutputCollector().add(tupleEntry);
        }
    }

    private void returnTuplesAsIs(BufferCall bufferCall) {
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext()) {
            bufferCall.getOutputCollector().add(iter.next());
        }
    }

    private String getStringAt(TupleEntry arguments, int pos) {
        Object obj = arguments.getObject(pos);
        if (obj == null) {
            return null;
        }
        if (obj instanceof Utf8) {
            return obj.toString();
        }
        return (String) obj;
    }

    private void setArgPosMap(TupleEntry arguments) {
        if (primaryDomainArgIdx == -1 || domainArgIdx == -1 || isPriDomArgIdx == -1) {
            Fields fields = arguments.getFields();
            if (primaryDomainArgIdx == -1) {
                primaryDomainArgIdx = fields.getPos(DomainCleanupByDuBuffer.DU_PRIMARY_DOMAIN);
            }
            if (domainArgIdx == -1) {
                domainArgIdx = fields.getPos(domainField);
            }
            if (isPriDomArgIdx == -1) {
                isPriDomArgIdx = fields.getPos(isPriDomField);
            }
        }
    }
}
