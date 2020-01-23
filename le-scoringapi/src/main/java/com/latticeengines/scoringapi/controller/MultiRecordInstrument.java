package com.latticeengines.scoringapi.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections4.CollectionUtils;
import org.aspectj.lang.reflect.MethodSignature;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.monitor.exposed.annotation.InvocationInstrument;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;

class MultiRecordInstrument implements InvocationInstrument {

    private final OAuthUserEntityMgr oAuthUserEntityMgr;

    MultiRecordInstrument(OAuthUserEntityMgr oAuthUserEntityMgr) {
        this.oAuthUserEntityMgr = oAuthUserEntityMgr;
    }

    @Override
    public String getTenantId(MethodSignature signature, Object[] args) {
        HttpServletRequest request = (HttpServletRequest) args[0];
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        return customerSpace.getTenantId();
    }

    @Override
    public double getNumReqs(MethodSignature signature, Object[] args, Object toReturn, Throwable ex) {
        BulkRecordScoreRequest scoreRequest = (BulkRecordScoreRequest) args[1];
        return CollectionUtils.size(scoreRequest.getRecords());
    }

    @Override
    public boolean hasErrorEvenNoException(MethodSignature signature, Object[] args, Object toReturn) {
        return getNumErrors(signature, args, toReturn) > 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public double getNumErrors(MethodSignature signature, Object[] args, Object toReturn) {
        List<RecordScoreResponse> responses = (List<RecordScoreResponse>) toReturn;
        if (CollectionUtils.isNotEmpty(responses)) {
            return (double) responses.stream() //
                    .filter(response -> CollectionUtils.isNotEmpty(response.getWarnings())) //
                    .count();
        } else {
            // consider all requests failed.
            return getNumReqs(signature, args, toReturn, null);
        }
    }

}
