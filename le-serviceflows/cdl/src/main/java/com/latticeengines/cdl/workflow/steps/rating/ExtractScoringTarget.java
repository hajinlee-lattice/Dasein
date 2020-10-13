package com.latticeengines.cdl.workflow.steps.rating;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("extractScoringTarget")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExtractScoringTarget extends BaseExtractRatingsStep<GenerateRatingStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExtractScoringTarget.class);

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private boolean hasCrossSellModel = false;

    @Override
    public void execute() {
        int count = retryCount();
        RetryTemplate retry = RetryUtils.getRetryTemplate(count);
        retry.execute(cxt -> {
            if (cxt.getRetryCount() > 0) {
                log.warn("(Attempt=" + cxt.getRetryCount() + " to execute. reset active version.");
                version = dataCollectionProxy.getActiveVersion(customerSpace.toString());
                WorkflowStaticContext.putObject(ATTRIBUTE_REPO, null);
            }
            setupExtractStep();
            containers.sort(Comparator.comparing(container -> container.getEngineSummary().getType()));
            containers.forEach(container -> {
                RatingEngineType ratingEngineType = container.getEngineSummary().getType();
                if (!hasCrossSellModel && RatingEngineType.CROSS_SELL.equals(ratingEngineType)) {
                    hasCrossSellModel = true;
                }
            });
            extractAllContainers();
            String resultTableName = NamingUtils.timestamp("ScoringTarget");
            mergeResults(resultTableName);
            removeObjectFromContext(FILTER_EVENT_TABLE);
            putStringValueInContext(FILTER_EVENT_TARGET_TABLE_NAME, resultTableName);
            putStringValueInContext(SCORING_UNIQUEKEY_COLUMN, InterfaceName.__Composite_Key__.name());
            putStringValueInContext(HAS_CROSS_SELL_MODEL, String.valueOf(hasCrossSellModel));
            return true;
        });
    }

    private int retryCount() {
        DataCollection.Version inactiveVesion = getObjectFromContext(CDL_INACTIVE_VERSION,
                DataCollection.Version.class);
        return inactiveVesion == null ? 2 : 1;
    }

    @Override
    protected boolean isRuleBased() {
        return false;
    }

    @Override
    protected List<RatingEngineType> getTargetEngineTypes() {
        return Arrays.asList(RatingEngineType.CROSS_SELL, RatingEngineType.CUSTOM_EVENT);
    }

    @Override
    protected HdfsDataUnit extractTargets(RatingModelContainer container) {
        RatingEngineSummary engineSummary = container.getEngineSummary();
        RatingEngineType engineType = engineSummary.getType();
        HdfsDataUnit result = null;
        if (RatingEngineType.CUSTOM_EVENT.equals(engineType)) {
            FrontEndQuery frontEndQuery = customEventQuery(engineSummary);
            result = getEntityQueryData(frontEndQuery);
        } else if (RatingEngineType.CROSS_SELL.equals(engineType)) {
            EventFrontEndQuery frontEndQuery = crossSellQuery(engineSummary.getId(), container.getModel().getId());
            result = getEventQueryData(frontEndQuery, EventType.Scoring);
        }
        return result;
    }

    @Override
    protected GenericRecord getDummyRecord() {
        Schema schema = AvroUtils.constructSchema("dummyScoringTarget",
                Arrays.asList(Pair.of(InterfaceName.AccountId.name(), String.class),
                        Pair.of(InterfaceName.PeriodId.name(), Long.class)));
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        String accountId = "__Dummy__Account__";
        builder.set(InterfaceName.AccountId.name(), accountId);
        builder.set(InterfaceName.PeriodId.name(), 0L);
        return builder.build();
    }

    private FrontEndQuery customEventQuery(RatingEngineSummary engineSummary) {
        MetadataSegment segment = segmentProxy.getMetadataSegmentByName(customerSpace.toString(), //
                engineSummary.getSegmentName());
        AttributeLookup accountId = new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name());
        FrontEndQuery frontEndQuery = segment.toFrontEndQuery(BusinessEntity.Account);
        frontEndQuery.setLookups(Collections.singletonList(accountId));
        return frontEndQuery;
    }

    private EventFrontEndQuery crossSellQuery(String engineId, String modelId) {
        return ratingEngineProxy.getModelingQueryByRatingId(customerSpace.toString(), engineId, modelId, //
                ModelingQueryType.TARGET);
    }

}
