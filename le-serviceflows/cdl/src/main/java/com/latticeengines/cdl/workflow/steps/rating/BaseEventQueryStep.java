package com.latticeengines.cdl.workflow.steps.rating;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.cdl.workflow.steps.export.BaseSparkSQLStep;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

public abstract class BaseEventQueryStep<T extends GenerateRatingStepConfiguration> extends BaseSparkSQLStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseEventQueryStep.class);

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    protected DataCollection.Version version;
    protected String evaluationDate;
    protected AttributeRepository attrRepo;
    protected List<RatingModelContainer> containers;

    protected void setupQueryStep() {
        customerSpace = parseCustomerSpace(configuration);
        version = parseDataCollectionVersion(configuration);
        attrRepo = parseAttrRepo(configuration);
        evaluationDate = parseEvaluationDateStr(configuration);
    }

    @Override
    protected CustomerSpace parseCustomerSpace(GenerateRatingStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected DataCollection.Version parseDataCollectionVersion(GenerateRatingStepConfiguration stepConfiguration) {
        if (version == null) {
            version = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
            if (version == null) {
                version = configuration.getDataCollectionVersion();
            }
            log.info("Using data collection version " + version);
        }
        return version;
    }

    @Override
    protected String parseEvaluationDateStr(GenerateRatingStepConfiguration stepConfiguration) {
        if (StringUtils.isBlank(evaluationDate)) {
            evaluationDate = getStringValueFromContext(CDL_EVALUATION_DATE);
            if (StringUtils.isBlank(evaluationDate)) {
                evaluationDate = periodProxy.getEvaluationDate(parseCustomerSpace(stepConfiguration).toString());
            }
        }
        return evaluationDate;
    }

    @Override
    protected AttributeRepository parseAttrRepo(GenerateRatingStepConfiguration stepConfiguration) {
        AttributeRepository attrRepo = WorkflowStaticContext.getObject(ATTRIBUTE_REPO, AttributeRepository.class);
        if (attrRepo == null) {
            attrRepo = constructAttrRepo();
            WorkflowStaticContext.putObject(ATTRIBUTE_REPO, attrRepo);
        }
        return attrRepo;
    }

    private AttributeRepository constructAttrRepo() {
        AttributeRepository attrRepo = dataCollectionProxy.getAttrRepo(customerSpace.toString(), version);
        insertPurchaseHistory(attrRepo);
        return attrRepo;
    }

    private void insertPurchaseHistory(AttributeRepository attrRepo) {
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), //
                TableRoleInCollection.CalculatedPurchaseHistory, version);
        if (table != null) {
            log.info("Insert purchase history table into attribute repository.");
            attrRepo.appendServingStore(BusinessEntity.PurchaseHistory, table);
        } else {
            log.warn("Did not find purchase history table in version " + version);
        }
    }

    protected void prepareEventQuery(boolean persistOnDisk) {
        String trxnTable = attrRepo.getTableName(TableRoleInCollection.AggregatedPeriodTransaction);
        if (StringUtils.isNotBlank(trxnTable)) {
            String period = configuration.getApsRollupPeriod();
            if (StringUtils.isBlank(period)) {
                period = "Month";
            }
            log.info("trxnTable=" + trxnTable + " period=" + period);
            prepareForCrossSellQueries(period, trxnTable, persistOnDisk);
        }
    }


}
