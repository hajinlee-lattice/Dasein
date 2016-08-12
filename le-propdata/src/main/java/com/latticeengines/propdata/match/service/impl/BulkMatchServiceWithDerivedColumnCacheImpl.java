package com.latticeengines.propdata.match.service.impl;

import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.propdata.core.service.PropDataTenantService;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.match.service.BulkMatchService;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("bulkMatchServiceWithDerivedColumnCache")
public class BulkMatchServiceWithDerivedColumnCacheImpl implements BulkMatchService {

    private static final String DEFAULT_VERSION_FOR_DERIVED_COLUMN_CACHE_BASED_MATCHING = "1.";

    private static Log log = LogFactory.getLog(BulkMatchServiceWithDerivedColumnCacheImpl.class);

    @Autowired
    protected MatchCommandService matchCommandService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected WorkflowProxy workflowProxy;

    @Autowired
    protected PropDataTenantService propDataTenantService;

    @Value("${propdata.match.max.num.blocks:4}")
    private Integer maxNumBlocks;

    @Value("${propdata.match.num.threads:4}")
    private Integer threadPoolSize;

    @Value("${propdata.match.bulk.group.size:20}")
    private Integer groupSize;

    @Value("${proxy.microservice.rest.endpoint.hostport}")
    protected String microserviceHostport;

    @Value("${propdata.match.average.block.size:2500}")
    private Integer averageBlockSize;

    @Override
    public boolean accept(String version) {
        if (StringUtils.isEmpty(version)
                || version.trim().startsWith(DEFAULT_VERSION_FOR_DERIVED_COLUMN_CACHE_BASED_MATCHING)) {
            return true;
        }

        return false;
    }

    @Override
    public MatchCommand match(MatchInput input, String hdfsPodId) {
        MatchInputValidator.validateBulkInput(input, yarnConfiguration);
        input.setMatchEngine(MatchContext.MatchEngine.BULK.getName());

        String rootOperationUid = UUID.randomUUID().toString().toUpperCase();

        if (StringUtils.isEmpty(hdfsPodId)) {
            hdfsPodId = CamilleEnvironment.getPodId();
        }
        log.info("PodId = " + hdfsPodId);
        HdfsPodContext.changeHdfsPodId(hdfsPodId);

        return submitBulkMatchWorkflow(input, hdfsPodId, rootOperationUid);
    }

    @Override
    public MatchCommand status(String rootOperationUid) {
        return matchCommandService.getByRootOperationUid(rootOperationUid);
    }

    protected MatchCommand submitBulkMatchWorkflow(MatchInput input, String hdfsPodId, String rootOperationUid) {
        propDataTenantService.bootstrapServiceTenant();
        BulkMatchWorkflowSubmitter submitter = new BulkMatchWorkflowSubmitter();
        ApplicationId appId = submitter //
                .matchInput(input) //
                .returnUnmatched(input.getReturnUnmatched()) //
                .hdfsPodId(hdfsPodId) //
                .rootOperationUid(rootOperationUid) //
                .workflowProxy(workflowProxy) //
                .microserviceHostport(microserviceHostport) //
                .averageBlockSize(averageBlockSize) //
                .excludePublicDomains(input.getExcludePublicDomains()) //
                .submit();
        return matchCommandService.start(input, appId, rootOperationUid);
    }

}
