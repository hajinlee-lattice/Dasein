package com.latticeengines.dataflow.runtime.cascading.propdata;


import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.DomainUtils;

import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DomainCleanupFunction extends CleanupFunction implements Function
{
    private static final long serialVersionUID = -4470533364538287281L;
    private String domainField;

    public DomainCleanupFunction(String domainField, boolean removeNull) {
        super(new Fields(domainField), removeNull);
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
