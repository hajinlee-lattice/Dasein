package com.latticeengines.apps.cdl.service;

import java.util.Map;

/**
 * Provide information of active stack in current environment
 */
public interface ActiveStackInfoService {

    /**
     * Check whether current stack is the active stack;
     *
     * @return
     */
    boolean isCurrentStackActive();

    /**
     * Retrieve the active stack name
     */
    String getActiveStack();

    /**
     * Retrieve all information about the active stack in current environment
     */
    Map<String, String> getActiveStackInfo();
}
