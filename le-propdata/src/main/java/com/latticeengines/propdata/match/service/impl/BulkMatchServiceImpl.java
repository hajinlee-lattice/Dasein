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

@Component("bulkMatchService")
public class BulkMatchServiceImpl implements BulkMatchService {

    private static Log log = LogFactory.getLog(BulkMatchServiceImpl.class);

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
    public MatchCommand match(MatchInput input, String hdfsPodId) {
        MatchInputValidator.validateBulkInput(input, yarnConfiguration);
        input.setMatchEngine(MatchContext.MatchEngine.BULK.getName());

        String uuid = UUID.randomUUID().toString().toUpperCase();

        if (StringUtils.isEmpty(hdfsPodId)) {
            hdfsPodId = CamilleEnvironment.getPodId();
        }
        log.info("PodId = " + hdfsPodId);
        HdfsPodContext.changeHdfsPodId(hdfsPodId);
        return submitMultipleBlockToWorkflow(input, hdfsPodId, uuid);
    }

    @Override
    public MatchCommand status(String rootOperationUid) {
        return matchCommandService.getByRootOperationUid(rootOperationUid);
    }

    private MatchCommand submitMultipleBlockToWorkflow(MatchInput input, String hdfsPodId, String uuid) {
        propDataTenantService.bootstrapServiceTenant();
        BulkMatchWorkflowSubmitter submitter = new BulkMatchWorkflowSubmitter();
        ApplicationId appId = submitter //
                .matchInput(input) //
                .returnUnmatched(input.getReturnUnmatched()) //
                .hdfsPodId(hdfsPodId) //
                .rootOperationUid(uuid) //
                .workflowProxy(workflowProxy) //
                .microserviceHostport(microserviceHostport) //
                .averageBlockSize(averageBlockSize) //
                .submit();
        return matchCommandService.start(input, appId, uuid);
    }

}
