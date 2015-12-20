package com.latticeengines.propdata.collection.dataflow.function;

import com.latticeengines.propdata.collection.util.DomainUtils;

import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DomainCleanupFunction extends CleanupFunction implements Function
{
    private String domainField;

    public DomainCleanupFunction(String domainField) {
        super(new Fields(domainField), true);
        this.domainField = domainField;
    }

    protected Tuple cleanupArguments(TupleEntry arguments) {
        String url = arguments.getString(domainField);
        try {
            String domain = DomainUtils.parseDomain(url.toLowerCase());
            if (domain == null) {
                return null;
            } else {
                return new Tuple(domain);
            }
        } catch (Exception e) {
            return null;
        }
    }

}
