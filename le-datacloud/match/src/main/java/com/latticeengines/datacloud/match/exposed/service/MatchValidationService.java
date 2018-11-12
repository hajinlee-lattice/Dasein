package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;

/*
 * Service to validate all match related entities
 */
public interface MatchValidationService {

    /**
     * Validate whether the specified decision graph exists or not.
     *
     * @param decisionGraph input decision graph name, if the value is {@literal null}, default decision graph will be
     *                      validated instead.
     * @throws IllegalArgumentException if the given graph does not exist
     */
    void validateDecisionGraph(String decisionGraph);

    /**
     * Validate whether the default decision graph exists or not.
     *
     * @throws IllegalArgumentException if the default graph does not exist
     */
    void validateDefaultDecisionGraph();

    /**
     * Validate whether the given data cloud version is valid to be used for matching
     *
     * @param dataCloudVersion data cloud version, cannot be {@literal null}
     * @throws RuntimeException if the given version cannot be used for matching
     */
    void validateDataCloudVersion(@NotNull String dataCloudVersion);
}
