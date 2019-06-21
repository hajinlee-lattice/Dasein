package com.latticeengines.cdl.workflow.steps.rating;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
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
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ExtractInactiveRatingsConfig;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.cdl.ExtractInactiveRatingsJob;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("extractInactiveRatings")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExtractInactiveRatings //
        extends RunSparkJob<GenerateRatingStepConfiguration, ExtractInactiveRatingsConfig> {

    private static final Logger log = LoggerFactory.getLogger(ExtractInactiveRatings.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    protected Class<ExtractInactiveRatingsJob> getJobClz() {
        return ExtractInactiveRatingsJob.class;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(GenerateRatingStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected ExtractInactiveRatingsConfig configureJob(GenerateRatingStepConfiguration stepConfiguration) {
        List<String> attrs = getListObjectFromContext(INACTIVE_ENGINE_ATTRIBUTES, String.class);
        if (CollectionUtils.isNotEmpty(attrs)) {
            log.info("Going to extract " + attrs.size() + " rating attrs for inactive ratings.");
            AttributeRepository attrRepo = WorkflowStaticContext.getObject(ATTRIBUTE_REPO, AttributeRepository.class);
            if (attrRepo == null) {
                throw new IllegalStateException("Cannot find attr repo in memory.");
            }
            String ratingTableName = attrRepo.getTableName(TableRoleInCollection.PivotedRating);
            if (StringUtils.isBlank(ratingTableName)) {
                log.warn("There is no PivotedRating table in attr repo, cannot extract inactive ratings.");
            } else {
                ExtractInactiveRatingsConfig config = new ExtractInactiveRatingsConfig();
                config.setInactiveRatingColumns(attrs);
                return config;
            }
        }
        return null;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String resultTableName = NamingUtils.timestamp("InactiveRatings");
        String pk = InterfaceName.__Composite_Key__.name();
        Table resultTable = toTable(resultTableName, pk, result.getTargets().get(0));
        metadataProxy.createTable(customerSpace.toString(), resultTableName, resultTable);
        putStringValueInContext(INACTIVE_RATINGS_TABLE_NAME, resultTableName);
        addToListInContext(TEMPORARY_CDL_TABLES, resultTableName, String.class);
    }

}
