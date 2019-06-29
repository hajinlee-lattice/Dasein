package com.latticeengines.cdl.workflow.steps.rating;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;

@Component("extractRuleBasedRatings")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExtractRuleBasedRatings extends BaseExtractRatingsStep<GenerateRatingStepConfiguration> {

    @Inject
    private SegmentProxy segmentProxy;

    @Override
    public void execute() {
        setupExtractStep();
        extractAllContainers();
        String resultTableName = NamingUtils.timestamp("RuleBased");
        mergeResults(resultTableName);
        putStringValueInContext(RULE_RAW_RATING_TABLE_NAME, resultTableName);
        addToListInContext(TEMPORARY_CDL_TABLES, resultTableName, String.class);
    }

    @Override
    protected boolean isRuleBased() {
        return true;
    }

    @Override
    protected List<RatingEngineType> getTargetEngineTypes() {
        return Collections.singletonList(RatingEngineType.RULE_BASED);
    }

    @Override
    protected HdfsDataUnit extractTargets(RatingModelContainer container) {
        RatingEngineSummary engineSummary = container.getEngineSummary();
        RatingEngineType engineType = engineSummary.getType();
        if (RatingEngineType.RULE_BASED.equals(engineType)) {
            String segmentName = engineSummary.getSegmentName();
            MetadataSegment segment = segmentProxy.getMetadataSegmentByName(customerSpace.toString(), segmentName);
            FrontEndQuery frontEndQuery = ruleBasedQuery(segment, container.getModel());
            String defaultBkt = ((RuleBasedModel) container.getModel()).getRatingRule().getDefaultBucketName();
            return getRuleBasedRatings(frontEndQuery, defaultBkt);
        } else {
            return null;
        }
    }

    @Override
    protected Schema getDummyRecordSchema() {
        return AvroUtils.constructSchema("dummyRating", Arrays.asList(
                Pair.of(InterfaceName.__Composite_Key__.name(), String.class),
                Pair.of(InterfaceName.AccountId.name(), String.class),
                Pair.of(InterfaceName.ModelId.name(), String.class),
                Pair.of(InterfaceName.Rating.name(), String.class)
        ));
    }

    private FrontEndQuery ruleBasedQuery(MetadataSegment segment, RatingModel ratingModel) {
        FrontEndQuery frontEndQuery = segment.toFrontEndQuery(BusinessEntity.Account);
        frontEndQuery.setRatingModels(Collections.singletonList(ratingModel));
        return frontEndQuery;
    }

}
