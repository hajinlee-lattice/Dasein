package com.latticeengines.matchapi.service.impl;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.datacloud.core.service.PropDataTenantService;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.datacloud.match.service.impl.MatchInputValidator;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.matchapi.service.BulkMatchService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("bulkMatchServiceWithDerivedColumnCache")
public class BulkMatchServiceWithDerivedColumnCacheImpl implements BulkMatchService {

    private static Logger log = LoggerFactory.getLogger(BulkMatchServiceWithDerivedColumnCacheImpl.class);

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

    @Value("${common.microservice.url}")
    protected String microserviceHostport;

    @Value("${datacloud.match.average.block.size:2500}")
    private Integer averageBlockSize;

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForRTSBasedMatch(version);
    }

    @Override
    public MatchCommand match(MatchInput input, String hdfsPodId) {
        MatchInputValidator.validateBulkInput(input, yarnConfiguration);
        input.setMatchEngine(MatchContext.MatchEngine.BULK.getName());
        String rootOperationUid = UUID.randomUUID().toString();
        hdfsPodId = setPodId(hdfsPodId);

        return submitBulkMatchWorkflow(input, hdfsPodId, rootOperationUid);
    }

    @Override
    public BulkMatchWorkflowConfiguration getWorkflowConf(MatchInput input, String hdfsPodId) {
        MatchInputValidator.validateBulkInput(input, yarnConfiguration);
        input.setMatchEngine(MatchContext.MatchEngine.BULK.getName());
        String rootOperationUid = UUID.randomUUID().toString().toUpperCase();
        hdfsPodId = setPodId(hdfsPodId);

        return generateWorkflowConf(input, hdfsPodId, rootOperationUid);
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
        BulkMatchWorkflowConfiguration configuration = generateWorkflowConf(input, hdfsPodId, rootOperationUid);
        AppSubmission appSubmission = workflowProxy.submitWorkflowExecution(configuration);
        ApplicationId appId = ConverterUtils.toApplicationId(appSubmission.getApplicationIds().get(0));
        return matchCommandService.start(input, appId, rootOperationUid);
    }

    protected BulkMatchWorkflowConfiguration generateWorkflowConf(MatchInput input, String hdfsPodId, String rootOperationUid) {
        BulkMatchWorkflowSubmitter submitter = new BulkMatchWorkflowSubmitter();
        return submitter //
                .matchInput(input) //
                .hdfsPodId(hdfsPodId) //
                .rootOperationUid(rootOperationUid) //
                .workflowProxy(workflowProxy) //
                .microserviceHostport(microserviceHostport) //
                .averageBlockSize(averageBlockSize) //
                .generateConfig();
    }

}
