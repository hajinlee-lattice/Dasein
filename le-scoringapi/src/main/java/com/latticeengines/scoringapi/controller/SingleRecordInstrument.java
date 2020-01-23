package com.latticeengines.scoringapi.controller;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections4.CollectionUtils;
import org.aspectj.lang.reflect.MethodSignature;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.monitor.exposed.annotation.InvocationInstrument;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;

class SingleRecordInstrument implements InvocationInstrument {

    private final OAuthUserEntityMgr oAuthUserEntityMgr;

    SingleRecordInstrument(OAuthUserEntityMgr oAuthUserEntityMgr) {
        this.oAuthUserEntityMgr = oAuthUserEntityMgr;
    }

    @Override
    public String getTenantId(MethodSignature signature, Object[] args) {
        HttpServletRequest request = (HttpServletRequest) args[0];
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        return customerSpace.getTenantId();
    }

    @Override
    public boolean hasErrorEvenNoException(MethodSignature signature, Object[] args, Object toReturn) {
        return getNumErrors(signature, args, toReturn) > 0;
    }

    @Override
    public double getNumErrors(MethodSignature signature, Object[] args, Object toReturn) {
        ScoreResponse response = (ScoreResponse) toReturn;
        if (response != null) {
            return CollectionUtils.isNotEmpty(response.getWarnings()) ? 1 : 0;
        } else {
            // consider the request failed.
            return 1;
        }
    }

}
