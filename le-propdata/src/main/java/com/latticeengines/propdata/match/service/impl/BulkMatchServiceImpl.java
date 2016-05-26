package com.latticeengines.propdata.match.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.propdata.PropDataJobConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.propdata.core.service.PropDataTenantService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.BulkMatchService;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.propdata.match.service.MatchPlanner;
import com.latticeengines.propdata.match.service.PropDataYarnService;
import com.latticeengines.propdata.match.util.MatchUtils;
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
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private PropDataYarnService yarnService;

    @Autowired
    private PropDataTenantService propDataTenantService;

    @Autowired
    @Qualifier(value = "bulkMatchPlanner")
    private MatchPlanner matchPlanner;

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

        if (input.getNumRows() > averageBlockSize) {
            log.info("Submitted rows " + input.getNumRows() + " is more than the average block size " + averageBlockSize
                    + ". Using parallel execution workflow container.");
            return submitMultipleBlockToWorkflow(input, hdfsPodId, uuid);
        } else {
            log.info("Submitted rows " + input.getNumRows() + " is less than the average block size " + averageBlockSize
                    + ". Using single execution yarn container.");
            return submitSingleBlockToYarn(input, hdfsPodId, uuid);
        }
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

    private MatchCommand submitSingleBlockToYarn(MatchInput input, String hdfsPodId, String uuid) {
        CustomerSpace customerSpace = CustomerSpace.parse(input.getTenant().getId());
        String inputAvro = hdfsPathBuilder.constructMatchBlockInputAvro(uuid, uuid).toString();

        PropDataJobConfiguration jobConfiguration = new PropDataJobConfiguration();
        jobConfiguration.setReturnUnmatched(input.getReturnUnmatched());
        jobConfiguration.setHdfsPodId(hdfsPodId);
        jobConfiguration.setName("PropDataMatchBlock");
        jobConfiguration.setCustomerSpace(customerSpace);
        jobConfiguration.setPredefinedSelection(input.getPredefinedSelection());
        jobConfiguration.setPredefinedSelectionVersion(input.getPredefinedVersion());
        jobConfiguration.setCustomizedSelection(input.getCustomSelection());
        jobConfiguration.setKeyMap(input.getKeyMap());
        jobConfiguration.setRootOperationUid(uuid);
        jobConfiguration.setGroupSize(groupSize);
        jobConfiguration.setThreadPoolSize(threadPoolSize);
        jobConfiguration.setBlockSize(input.getNumRows());
        jobConfiguration.setBlockOperationUid(uuid);
        jobConfiguration.setAvroPath(inputAvro);
        jobConfiguration.setAppName(String.format("PropDataMatch[%s]~" + customerSpace.toString(), uuid));
        jobConfiguration.setSingleBlock(true);
        jobConfiguration.setYarnQueue(input.getYarnQueue());

        copyInputData(input, uuid);

        ApplicationId appId = yarnService.submitPropDataJob(jobConfiguration);
        return matchCommandService.start(input, appId, uuid);
    }

    @MatchStep
    private void copyInputData(MatchInput input, String rootOperationUid) {
        AvroInputBuffer avroInputBuffer = (AvroInputBuffer) input.getInputBuffer();
        String inputDir = avroInputBuffer.getAvroDir();
        String matchInputAvro = hdfsPathBuilder.constructMatchBlockInputAvro(rootOperationUid, rootOperationUid)
                .toString();
        String avroGlobs = MatchUtils.toAvroGlobs(inputDir);
        Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, avroGlobs);
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlobs);
        List<GenericRecord> data = new ArrayList<>();
        while (iterator.hasNext()) {
            data.add(iterator.next());
        }
        try {
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, matchInputAvro, data);
            log.info("Write a block of " + AvroUtils.count(yarnConfiguration, matchInputAvro) + " rows to "
                    + matchInputAvro);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
