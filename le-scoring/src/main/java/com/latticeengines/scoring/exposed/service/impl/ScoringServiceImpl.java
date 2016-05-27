package com.latticeengines.scoring.exposed.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorUtil;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringProperty;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.scoring.exposed.domain.ScoringRequest;
import com.latticeengines.scoring.exposed.domain.ScoringResponse;
import com.latticeengines.scoring.exposed.service.ScoringService;

@Component("scoringService")
public class ScoringServiceImpl implements ScoringService {

    private static final String SCORING_CLIENT = "scoringClient";

    @Autowired
    private JobService jobService;

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private AsyncTaskExecutor asyncTaskExecutor;

    @Override
    public List<ScoringResponse> scoreBatch(List<ScoringRequest> scoringRequests, PMML pmml) {
        List<Future<ScoringResponse>> futures = new ArrayList<Future<ScoringResponse>>();

        for (ScoringRequest scoringRequest : scoringRequests) {
            futures.add(scoreAsync(scoringRequest, pmml));
        }
        List<ScoringResponse> responses = new ArrayList<ScoringResponse>();

        for (Future<ScoringResponse> future : futures) {
            ScoringResponse scoringResponse = null;
            try {
                scoringResponse = future.get();

            } catch (Exception e) {
                scoringResponse.setError(ExceptionUtils.getFullStackTrace(e));
            }
            responses.add(scoringResponse);
        }

        return responses;
    }

    private Future<ScoringResponse> scoreAsync(final ScoringRequest scoringRequest, final PMML pmml) {
        return asyncTaskExecutor.submit(new Callable<ScoringResponse>() {

            @Override
            public ScoringResponse call() throws Exception {
                return score(scoringRequest, pmml);
            }
        });
    }

    @Override
    public ScoringResponse score(ScoringRequest scoringRequest, PMML pmml) {
        PMMLManager pmmlManager = new PMMLManager(pmml);
        Evaluator evaluator = (Evaluator) pmmlManager.getModelManager(null, ModelEvaluatorFactory.getInstance());
        Map<FieldName, Object> arguments = new LinkedHashMap<FieldName, Object>();

        List<FieldName> activeFields = evaluator.getActiveFields();
        for (FieldName activeField : activeFields) {
            Object value = scoringRequest.getArgument(activeField.getValue());
            arguments.put(activeField, EvaluatorUtil.prepare(evaluator, activeField, value));
        }

        Map<FieldName, ?> result = evaluator.evaluate(arguments);
        ScoringResponse response = new ScoringResponse();
        Map<String, Object> resultMap = new HashMap<String, Object>();
        for (Entry<FieldName, ?> entry : result.entrySet()) {
            resultMap.put(entry.getKey().getValue(), entry.getValue());
        }
        response.setResult(resultMap);
        return response;
    }

    @Override
    public ApplicationId submitScoreWorkflow(RTSBulkScoringConfiguration rtsBulkScoringConfig) {
        Job job = createJob(rtsBulkScoringConfig);
        ApplicationId appId = jobService.submitJob(job);
        job.setId(appId.toString());
        jobEntityMgr.create(job);
        return appId;
    }

    private Job createJob(RTSBulkScoringConfiguration rtsBulkScoringConfig) {

        Job job = new Job();
        String customerSpace = rtsBulkScoringConfig.getCustomerSpace().toString();

        job.setClient(SCORING_CLIENT);
        job.setCustomer(customerSpace);

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(),
                String.format("customer%s", String.valueOf(System.currentTimeMillis())));
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getScoringQueueNameForSubmission());

        Properties containerProperties = new Properties();
        containerProperties.put(RTSBulkScoringProperty.RTS_BULK_SCORING_CONFIG, rtsBulkScoringConfig.toString());
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "1096");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");

        job.setAppMasterPropertiesObject(appMasterProperties);
        job.setContainerPropertiesObject(containerProperties);

        return job;
    }
}
