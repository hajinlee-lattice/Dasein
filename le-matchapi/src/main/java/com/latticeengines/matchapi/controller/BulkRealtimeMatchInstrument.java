package com.latticeengines.matchapi.controller;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.reflect.MethodSignature;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.monitor.exposed.annotation.InvocationInstrument;

public class BulkRealtimeMatchInstrument implements InvocationInstrument {

    static final String NAME = "match-bulk-realtime";
    static final String NAME_MATCHED = "match-bulk-realtime-matched";

    private final boolean onlyCountMatched;

    BulkRealtimeMatchInstrument(boolean onlyCountMatched) {
        this.onlyCountMatched = onlyCountMatched;
    }

    @Override
    public String getTenantId(MethodSignature signature, Object[] args) {
        String tenantId = InvocationInstrument.UNKOWN_TAG_VALUE;
        BulkMatchInput bulkMatchInput = (BulkMatchInput) args[0];
        if (bulkMatchInput != null && CollectionUtils.isNotEmpty(bulkMatchInput.getInputList())) {
            for (MatchInput input: bulkMatchInput.getInputList()) {
                Tenant tenant1 = input.getTenant();
                if (tenant1 != null) {
                    tenantId = CustomerSpace.shortenCustomerSpace(tenant1.getId());
                    if (StringUtils.isNotBlank(tenantId)) {
                        break;
                    }
                }
            }
        }
        return tenantId;
    }

    @Override
    public double getNumReqs(MethodSignature signature, Object[] args, Object toReturn, Throwable ex) {
        if (onlyCountMatched) {
            return getMatchedRows(toReturn, ex);
        } else {
            return getRequestedRows(args);
        }
    }

    private long getRequestedRows(Object[] args) {
        BulkMatchInput bulkMatchInput = (BulkMatchInput) args[0];
        if (bulkMatchInput != null && CollectionUtils.isNotEmpty(bulkMatchInput.getInputList())) {
            return bulkMatchInput.getInputList().stream().mapToInt(MatchInput::getNumRows).sum();
        } else {
            return 0;
        }
    }

    private long getMatchedRows(Object toReturn, Throwable ex) {
        long matched = 0;
        if (ex == null && toReturn != null) {
            BulkMatchOutput bulkMatchOutput = (BulkMatchOutput) toReturn;
            List<MatchOutput> outputs = bulkMatchOutput.getOutputList();
            if (CollectionUtils.isNotEmpty(outputs)) {
                matched = outputs.stream().mapToLong(output -> output.getStatistics().getRowsMatched()).sum();
            }
        }
        return matched;
    }

}
