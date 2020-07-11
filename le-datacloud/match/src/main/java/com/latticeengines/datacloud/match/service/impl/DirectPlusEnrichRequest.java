package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;

public class DirectPlusEnrichRequest {

    private String dunsNumber;
    private Set<String> blockIds;
    private List<PrimeColumn> reqColumns;

    public String getDunsNumber() {
        return dunsNumber;
    }

    public void setDunsNumber(String dunsNumber) {
        this.dunsNumber = dunsNumber;
    }

    public Set<String> getBlockIds() {
        return blockIds;
    }

    public void setBlockIds(Set<String> blockIds) {
        this.blockIds = blockIds;
    }

    public List<PrimeColumn> getReqColumns() {
        return reqColumns;
    }

    public void setReqColumns(List<PrimeColumn> reqColumns) {
        this.reqColumns = reqColumns;
    }
}
