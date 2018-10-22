package com.latticeengines.domain.exposed.dataflow.operations;

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

/**
 * To build operation logs in dataflow. Currently only DataCloud pipeline uses
 * it. But it could be used in any dataflow. So put in the package not specific
 * to DataCloud
 */
public class OperationMessage {
    /*
     * AccountMasterSeed primary domain selection
     */
    public static final String ONLY_ONE_DOMAIN = "Only one domain";
    public static final String DOMAIN_SRC = "DomainSource=";
    public static final String LOW_ALEXA_RANK = "Lowest AlexaRank=";
    public static final String ORI_PRIDOM_FLAG = "Original " + DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN + "=";
}
