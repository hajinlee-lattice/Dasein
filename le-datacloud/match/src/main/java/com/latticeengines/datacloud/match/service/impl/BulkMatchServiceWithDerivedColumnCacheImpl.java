package com.latticeengines.datacloud.match.service.impl;

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
import com.latticeengines.datacloud.match.exposed.service.BulkMatchService;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;
import com.latticeengines.propdata.core.service.PropDataTenantService;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("bulkMatchServiceWithDerivedColumnCache")
public class BulkMatchServiceWithDerivedColumnCacheImpl implements BulkMatchService {

    private static Log log = LogFactory.getLog(BulkMatchServiceWithDerivedColumnCacheImpl.class);

    @Autowired
    protected MatchCommandService matchCommandService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected WorkflowProxy workflowProxy;

    @Autowired
    protected PropDataTenantService propDataTenantService;

    @Value("${datacloud.match.max.num.blocks:4}")
    private Integer maxNumBlocks;

    @Value("${datacloud.match.num.threads:4}")
    private Integer threadPoolSize;

    @Value("${datacloud.match.bulk.group.size:20}")
    private Integer groupSize;

    @Value("${proxy.microservice.rest.endpoint.hostport}")
    protected String microserviceHostport;

    @Value("${datacloud.match.average.block.size:2500}")
    private Integer averageBlockSize;

    @Override
    public boolean accept(String version) {
        return MatchTypeUtil.isValidForRTSBasedMatch(version);
    }

    @Override
    public MatchCommand match(MatchInput input, String hdfsPodId) {
        MatchInputValidator.validateBulkInput(input, yarnConfiguration);
        input.setMatchEngine(MatchContext.MatchEngine.BULK.getName());
        String rootOperationUid = UUID.randomUUID().toString().toUpperCase();
        hdfsPodId = setPodId(hdfsPodId);

        return submitBulkMatchWorkflow(input, hdfsPodId, rootOperationUid);
    }

    protected String setPodId(String hdfsPodId) {
        if (StringUtils.isEmpty(hdfsPodId)) {
            hdfsPodId = CamilleEnvironment.getPodId();
        }
        log.info("PodId = " + hdfsPodId);
        HdfsPodContext.changeHdfsPodId(hdfsPodId);
        return hdfsPodId;
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
