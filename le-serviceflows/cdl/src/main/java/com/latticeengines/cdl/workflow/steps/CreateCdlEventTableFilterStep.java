package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.rating.BaseEventQueryStep;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableFilterConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CreateEventTableFilterJobConfig;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.spark.exposed.job.cdl.CreateEventTableFilterJob;

@Component("createCdlEventTableFilterStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CreateCdlEventTableFilterStep extends BaseEventQueryStep<CreateCdlEventTableFilterConfiguration> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CreateCdlEventTableFilterStep.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        super.setupQueryStep();
        executeSparkJob();

    }

    private SparkJobResult executeSparkJob() {

        RetryTemplate retry = RetryUtils.getRetryTemplate(2);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                log.warn("Previous failure:", ctx.getLastThrowable());
            }

            try {
                startSparkSQLSession(getHdfsPaths(attrRepo), false);
                prepareEventQuery(true);
                HdfsDataUnit trainData = getEventQueryData(configuration.getTrainQuery(), EventType.Training);
                EventFrontEndQuery eventQuery = configuration.getEventQuery();
                eventQuery.setCalculateProductRevenue(configuration.isExpectedValue());
                HdfsDataUnit eventData = getEventQueryData(eventQuery, EventType.Event);
                CreateEventTableFilterJobConfig config = getConfig(trainData, eventData);
                SparkJobResult result = executeSparkJob(CreateEventTableFilterJob.class, config);
                log.info("CreateCdlEventTableFilterStep Results: " + JsonUtils.serialize(result));
                setupResult(result);
                return result;
            } finally {
                stopSparkSQLSession();
            }
        });
    }

    private CreateEventTableFilterJobConfig getConfig(HdfsDataUnit trainData, HdfsDataUnit eventData) {
        CreateEventTableFilterJobConfig config = new CreateEventTableFilterJobConfig();
        config.setEventColumn(configuration.getEventColumn());
        config.setInput(Arrays.asList(trainData, eventData));
        config.setWorkspace(getRandomWorkspace());
        return config;
    }

    public void setupResult(SparkJobResult result) {
        String targetTableName = NamingUtils.timestampWithRandom("CreateCdlEventTableFilter");
        Table resultTable = toTable(targetTableName, result.getTargets().get(0));
        metadataProxy.createTable(customerSpace.toString(), targetTableName, resultTable);
        putObjectInContext(FILTER_EVENT_TABLE, resultTable);
    }

}
