package com.latticeengines.propdata.match.service.impl;

import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.match.service.BulkMatchService;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("bulkMatchService")
public class BulkMatchServiceImpl implements BulkMatchService {

    @Autowired
    private MatchCommandService matchCommandService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Value("${propdata.match.group.size:100}")
    private Integer groupSize;

    @Value("${proxy.microservice.rest.endpoint.hostport}")
    private String microserviceHostport;

    @Override
    public MatchCommand match(MatchInput input, String hdfsPodId) {
        MatchInputValidator.validateBulkInput(input, yarnConfiguration);
        String uuid = UUID.randomUUID().toString().toUpperCase();

        if (StringUtils.isEmpty(hdfsPodId)) {
            hdfsPodId = CamilleEnvironment.getPodId();
        }

        BulkMatchWorkflowSubmitter submitter = new BulkMatchWorkflowSubmitter();
        ApplicationId appId = submitter //
                .matchInput(input) //
                .hdfsPodId(hdfsPodId) //
                .groupSize(groupSize) //
                .rootOperationUid(uuid) //
                .workflowProxy(workflowProxy) //
                .inputDir(hdfsPathBuilder.constructMatchInputDir(uuid).toString()) //
                .microserviceHostport(microserviceHostport) //
                .submit();

        return matchCommandService.start(input, appId, uuid);
    }

}
