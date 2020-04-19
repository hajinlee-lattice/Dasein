package com.latticeengines.cdl.workflow.steps.rating;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ConcatenateAIRatingsConfig;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.cdl.ConcatenateAIRatingsJob;

@Component("mergePythonVersions")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergePythonVersions extends RunSparkJob<GenerateRatingStepConfiguration, ConcatenateAIRatingsConfig> {

    private final Logger log = LoggerFactory.getLogger(MergePythonVersions.class);

    @Override
    protected Class<ConcatenateAIRatingsJob> getJobClz() {
        return ConcatenateAIRatingsJob.class;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(GenerateRatingStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected ConcatenateAIRatingsConfig configureJob(GenerateRatingStepConfiguration stepConfiguration) {
        ConcatenateAIRatingsConfig jobConfig = null;
        String p2ResultTable = getStringValueFromContext(AI_RAW_RATING_TABLE_NAME_P2);
        String p3ResultTable = getStringValueFromContext(AI_RAW_RATING_TABLE_NAME);
        if (StringUtils.isNotBlank(p2ResultTable) && StringUtils.isNotBlank(p3ResultTable)) {
            log.info("Merging python 2 result {} and python 3 result {}", p2ResultTable, p3ResultTable);
            String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
            Table p2Table = metadataProxy.getTable(tenantId, p2ResultTable);
            if (p2Table == null) {
                throw new IllegalArgumentException("Cannot find python 2 result table " + p2ResultTable);
            }
            HdfsDataUnit p2Input = p2Table.toHdfsDataUnit("python2");
            Table p3Table = metadataProxy.getTable(tenantId, p3ResultTable);
            if (p3Table == null) {
                throw new IllegalArgumentException("Cannot find python 3 result table " + p3ResultTable);
            }
            HdfsDataUnit p3Input = p3Table.toHdfsDataUnit("python3");
            jobConfig = new ConcatenateAIRatingsConfig();
            jobConfig.setInput(Arrays.asList(p2Input, p3Input));
        } else if (StringUtils.isNotBlank(p2ResultTable)) {
            log.info("Only found python 2 result {}", p2ResultTable);
            putStringValueInContext(AI_RAW_RATING_TABLE_NAME, p2ResultTable);
        } else if (StringUtils.isNotBlank(p3ResultTable)) {
            log.info("Only found python 3 result {}", p2ResultTable);
        } else {
            log.info("Found neither python 2 nor python 3 result!");
            removeObjectFromContext(AI_RAW_RATING_TABLE_NAME);
        }
        removeObjectFromContext(AI_RAW_RATING_TABLE_NAME_P2);
        removeObjectFromContext(PYTHON_MAJOR_VERSION);
        return jobConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
        String resultTableName = tenantId + "_" + NamingUtils.timestamp("ScoreResult");
        String pk = InterfaceName.AccountId.name();
        Table resultTable = toTable(resultTableName, pk, result.getTargets().get(0));
        metadataProxy.createTable(customerSpace.toString(), resultTableName, resultTable);
        putStringValueInContext(AI_RAW_RATING_TABLE_NAME, resultTableName);
    }

}
