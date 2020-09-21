package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;

public class DirectPlusEnrichRequest {

    private String dunsNumber;
    private Map<String, List<PrimeColumn>> reqColumnsByBlockId;
    private boolean bypassDplusCache;

    public String getDunsNumber() {
        return dunsNumber;
    }

    public void setDunsNumber(String dunsNumber) {
        this.dunsNumber = dunsNumber;
    }

    public Map<String, List<PrimeColumn>> getReqColumnsByBlockId() {
        return reqColumnsByBlockId;
    }

    public void setReqColumnsByBlockId(Map<String, List<PrimeColumn>> reqColumnsByBlockId) {
        this.reqColumnsByBlockId = reqColumnsByBlockId;
    }

    public boolean isBypassDplusCache() {
        return bypassDplusCache;
    }

    public void setBypassDplusCache(boolean bypassDplusCache) {
        this.bypassDplusCache = bypassDplusCache;
    }

}
