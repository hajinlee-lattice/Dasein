package com.latticeengines.propdata.core.dataflow.function;


import org.apache.commons.lang.StringUtils;

import com.latticeengines.propdata.core.util.DomainUtils;

import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DomainCleanupFunction extends CleanupFunction implements Function
{
    private static final long serialVersionUID = -4470533364538287281L;
    private String domainField;

    public DomainCleanupFunction(String domainField) {
        super(new Fields(domainField), true);
        this.domainField = domainField;
    }

    @Override
    protected Tuple cleanupArguments(TupleEntry arguments) {
        String url = arguments.getString(domainField);
        try {
            String domain = DomainUtils.parseDomain(url.toLowerCase());
            if (StringUtils.isEmpty(domain)) {
                return null;
            } else {
                return new Tuple(domain);
            }
        } catch (Exception e) {
            return null;
        }
    }

}
