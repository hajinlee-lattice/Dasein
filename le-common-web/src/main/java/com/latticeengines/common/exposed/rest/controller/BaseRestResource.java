package com.latticeengines.common.exposed.rest.controller;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;


/**
 * Base Resource that will inherit some common functionality like
 * 1. Creating Pageable Requests
 * 2. Add common resources ex: POST ../logging (Dynamically controls log level) 
 * 3. Adding healthCheck
 *
 */
public class BaseRestResource {

    private static final Logger log = LoggerFactory.getLogger(BaseRestResource.class);
    
    /**
     * @param pageIndex
     * @return zero-based page index
     */
    protected int getZeroBasedPage(int pageIndex) {
        pageIndex = (pageIndex < 1 ) ? 0 : (pageIndex - 1);
        return pageIndex;
    }
    
    protected Pageable getPageRequest(Integer pageIndex, Integer size) {
        String[] sortColumns = {};
        return getPageRequest(pageIndex, size, null, sortColumns);
    }
    
    protected Pageable getPageRequest(Integer pageIndex, Integer size, String... sortColumns) {
        return getPageRequest(pageIndex, size, null, sortColumns);
    }
    
    protected Pageable getPageRequest(Integer pageIndex, Integer size, Direction sortDirection, String... sortColumns) {
        if (pageIndex == null || size == null || pageIndex <= 0 || size <= 0) {
            return null;
        }
        
        if (sortColumns != null && sortColumns.length > 0) {
            sortColumns = Arrays.stream(sortColumns).filter(col -> StringUtils.isNotBlank(col)).toArray(String[]::new);
        }
        
        Sort sortCriteria = null;
        if (sortColumns != null && sortColumns.length > 0) {
            sortCriteria = sortDirection != null ? Sort.by(sortDirection, sortColumns) : Sort.by(sortColumns);
        } else {
            sortCriteria = Sort.unsorted();
        }
        PageRequest pageRequest = PageRequest.of(getZeroBasedPage(pageIndex), size, sortCriteria);
        if (log.isDebugEnabled()) {
            log.debug("PageRequest " + pageRequest);
        }
        return pageRequest;
    }

}
