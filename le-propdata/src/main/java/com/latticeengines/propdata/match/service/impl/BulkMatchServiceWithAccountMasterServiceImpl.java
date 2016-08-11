package com.latticeengines.propdata.match.service.impl;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.propdata.core.service.PropDataTenantService;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("bulkMatchServiceWithAccountMaster")
public class BulkMatchServiceWithAccountMasterServiceImpl extends BulkMatchServiceWithDerivedColumnCacheImpl {

    private static Log log = LogFactory.getLog(BulkMatchServiceWithAccountMasterServiceImpl.class);

    private static final String DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING = "2.";

    @Autowired
    private MatchCommandService matchCommandService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private PropDataTenantService propDataTenantService;

    @Value("${propdata.match.max.num.blocks:4}")
    private Integer maxNumBlocks;

    @Value("${propdata.match.num.threads:4}")
    private Integer threadPoolSize;

    @Value("${propdata.match.bulk.group.size:20}")
    private Integer groupSize;

    @Value("${proxy.microservice.rest.endpoint.hostport}")
    private String microserviceHostport;

    @Value("${propdata.match.average.block.size:2500}")
    private Integer averageBlockSize;

    @Override
    public boolean accept(String version) {
        if (!StringUtils.isEmpty(version)
                && version.trim().startsWith(DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING)) {
            return true;
        }

        return false;
    }

    @Override
    public MatchCommand match(MatchInput input, String hdfsPodId) {
        // TODO - need to be implemented by Lei
        throw new NotImplementedException("Impl of account master based match is yet to be implemented");
    }

    @Override
    public MatchCommand status(String rootOperationUid) {
        // TODO - need to be implemented by Lei
        throw new NotImplementedException("Impl of account master based match is yet to be implemented");
    }

}
