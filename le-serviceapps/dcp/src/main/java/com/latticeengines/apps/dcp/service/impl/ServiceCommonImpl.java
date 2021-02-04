package com.latticeengines.apps.dcp.service.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import com.google.common.base.Preconditions;

public class ServiceCommonImpl {

    @Value("${dcp.app.project.pagesize}")
    private int maxPageSize;

    protected PageRequest getPageRequest(int pageIndex, int pageSize) {
        Preconditions.checkState(pageIndex >= 0);
        Preconditions.checkState(pageSize > 0);
        if (pageSize > maxPageSize) {
            pageSize = maxPageSize;
        }
        Sort sort = Sort.by(Sort.Direction.DESC, "updated");
        return PageRequest.of(pageIndex, pageSize, sort);
    }

}
