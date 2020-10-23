package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.Collections;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.export.BaseSparkSQLStep;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateTimelineExportUniverseStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("generateTimelineUniverse")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateTimelineExportUniverse extends BaseSparkSQLStep<GenerateTimelineExportUniverseStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(GenerateTimelineExportUniverse.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private DataCollection.Version version;
    private AttributeRepository attrRepo;

    @Override
    protected CustomerSpace parseCustomerSpace(GenerateTimelineExportUniverseStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected DataCollection.Version parseDataCollectionVersion(GenerateTimelineExportUniverseStepConfiguration stepConfiguration) {
        if (version == null) {
            version = configuration.getVersion();
        }
        return version;
    }

    @Override
    protected String parseEvaluationDateStr(GenerateTimelineExportUniverseStepConfiguration stepConfiguration) {
        return null;
    }

    @Override
    protected AttributeRepository parseAttrRepo(GenerateTimelineExportUniverseStepConfiguration stepConfiguration) {
        AttributeRepository attrRepo = WorkflowStaticContext.getObject(ATTRIBUTE_REPO, AttributeRepository.class);
        if (attrRepo == null) {
            attrRepo = dataCollectionProxy.getAttrRepo(customerSpace.toString(), version);
            insertPurchaseHistory(attrRepo);
        }
        return attrRepo;
    }

    protected FrontEndQuery getAccountFiltererSegmentQuery() {
        FrontEndQuery fe = FrontEndQuery.fromSegment(configuration.getMetadataSegment());
        fe.setMainEntity(BusinessEntity.Account);
        fe.setLookups( //
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())));
        fe.setSort(new FrontEndSort(
                Collections.singletonList(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())),
                false));
        return fe;
    }

    @Override
    public void execute() {
        if (configuration.getMetadataSegment() == null) {
            log.info("can't find valid segment, skip this step.");
            return;
        }
        customerSpace = parseCustomerSpace(configuration);
        version = parseDataCollectionVersion(configuration);
        attrRepo = parseAttrRepo(configuration);
        FrontEndQuery query = getAccountFiltererSegmentQuery();
        log.info("Full Launch Universe Query: {}.", query.toString());

        HdfsDataUnit timelineUniverseDataUnit = executeSparkJob(query);
        log.info(getHDFSDataUnitLogEntry("CurrentTimelineUniverse", timelineUniverseDataUnit));
        putObjectInContext(TIMELINE_EXPORT_ACCOUNTLIST, timelineUniverseDataUnit);
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

    private HdfsDataUnit executeSparkJob(FrontEndQuery frontEndQuery) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(2);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt={}) extract entities via Spark SQL.", (ctx.getRetryCount() + 1));
                log.warn("Previous failure:", ctx.getLastThrowable());
            }
            try {
                startSparkSQLSession(getHdfsPaths(attrRepo), false);
                HdfsDataUnit timelineDataUniverseDataUnit = getEntityQueryData(frontEndQuery, true);

                log.info("DataUnit: {}.", JsonUtils.serialize(timelineDataUniverseDataUnit));
                return timelineDataUniverseDataUnit;
            } finally {
                stopSparkSQLSession();
            }
        });

    }

    private String getHDFSDataUnitLogEntry(String tag, HdfsDataUnit dataUnit) {
        if (dataUnit == null) {
            return tag + " data set empty";
        }
        return String.format("%s, %s", tag, JsonUtils.serialize(dataUnit));
    }
}
