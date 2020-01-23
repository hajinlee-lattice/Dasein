package com.latticeengines.monitor.exposed.annotation;

import java.util.Collections;
import java.util.List;

import org.aspectj.lang.reflect.MethodSignature;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;

public interface InvocationInstrument {

    String UNKOWN_TAG_VALUE = "__UNKNOWN__";

    // to filter based on url path variables
    default boolean accept(MethodSignature signature, Object[] args, Object toReturn, Throwable ex) {
        return true;
    }

    default String getTenantId(MethodSignature signature, Object[] args) {
        String tenantId = UNKOWN_TAG_VALUE;
        String[] paramsNames = signature.getParameterNames();
        for (int i= 0; i < paramsNames.length; i++) {
            String paramName = paramsNames[i];
            Object arg = args[i];
            if (arg != null && (
                    "customerSpace".equalsIgnoreCase(paramName) //
                    || "tenant".equalsIgnoreCase(paramName) //
                    || "tenantId".equalsIgnoreCase(paramName) //
            )) {
                if (arg instanceof CustomerSpace) {
                    tenantId = ((CustomerSpace) arg).getTenantId();
                } else if (arg instanceof Tenant) {
                    tenantId = CustomerSpace.shortenCustomerSpace(((Tenant) arg).getId());
                } else if (arg instanceof String) {
                    tenantId = CustomerSpace.shortenCustomerSpace((String) arg);
                }
            }
        }
        return tenantId;
    }

    default double getNumReqs(MethodSignature signature, Object[] args, Object toReturn, Throwable ex) {
        return 1;
    }

    // avoid using generic tags, as it will drastically expand the cardinality
    default List<String> getGenericTagValues(MethodSignature signature, Object[] args, Object toReturn, Throwable ex) {
        return Collections.emptyList();
    }

    // only needed if track errors in separate measurement
    default double getNumErrors(MethodSignature signature, Object[] args, Object toReturn) {
        return 0.0;
    }

    // only needed if track errors in separate measurement
    default double getNumErrors(MethodSignature signature, Object[] args, Throwable ex) {
        return getNumReqs(signature, args, null, ex);
    }

}
