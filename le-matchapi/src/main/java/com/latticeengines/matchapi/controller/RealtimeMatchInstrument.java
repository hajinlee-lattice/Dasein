package com.latticeengines.matchapi.controller;

import org.aspectj.lang.reflect.MethodSignature;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.monitor.exposed.annotation.InvocationInstrument;

public class RealtimeMatchInstrument implements InvocationInstrument {

    static final String NAME = "match-realtime";
    static final String NAME_MATCHED = "match-realtime-matched";

    private final boolean onlyCountMatched;

    RealtimeMatchInstrument(boolean onlyCountMatched) {
        this.onlyCountMatched = onlyCountMatched;
    }

    @Override
    public String getTenantId(MethodSignature signature, Object[] args) {
        MatchInput matchInput = (MatchInput) args[0];
        if (matchInput != null) {
            Tenant tenant = matchInput.getTenant();
            if (tenant != null) {
                return CustomerSpace.shortenCustomerSpace(tenant.getId());
            }
        }
        return InvocationInstrument.UNKOWN_TAG_VALUE;
    }

    @Override
    public double getNumReqs(MethodSignature signature, Object[] args, Object toReturn, Throwable ex) {
        if (onlyCountMatched) {
            return isMatched(toReturn, ex) ? 1 : 0;
        } else {
            return 1;
        }
    }

    private boolean isMatched(Object toReturn, Throwable ex) {
        if (ex != null || toReturn == null) {
            return false;
        } else {
            MatchOutput matchOutput = (MatchOutput) toReturn;
            return matchOutput.getStatistics().getRowsMatched() > 0;
        }
    }

}
