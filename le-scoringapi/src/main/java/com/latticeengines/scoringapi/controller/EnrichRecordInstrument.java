package com.latticeengines.scoringapi.controller;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.aspectj.lang.reflect.MethodSignature;

import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponse;
import com.latticeengines.monitor.exposed.annotation.InvocationInstrument;

class EnrichRecordInstrument implements InvocationInstrument {

    EnrichRecordInstrument() {
    }

    @Override
    public String getTenantId(MethodSignature signature, Object[] args) {
        String uuid = (String) args[1];
        Pair<String, String> unpacked = UuidUtils.unpackPairUuid(uuid);
        String tenantId = unpacked.getKey();
        return CustomerSpace.shortenCustomerSpace(tenantId);
    }

    @Override
    public boolean hasErrorEvenNoException(MethodSignature signature, Object[] args, Object toReturn) {
        return getNumErrors(signature, args, toReturn) > 0;
    }

    @Override
    public double getNumErrors(MethodSignature signature, Object[] args, Object toReturn) {
        EnrichResponse response = (EnrichResponse) toReturn;
        if (response != null) {
            return CollectionUtils.isNotEmpty(response.getWarnings()) ? 1 : 0;
        } else {
            // consider the request failed.
            return 1;
        }
    }

}
