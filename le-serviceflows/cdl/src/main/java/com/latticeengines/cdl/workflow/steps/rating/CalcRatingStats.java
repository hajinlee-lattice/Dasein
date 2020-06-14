package com.latticeengines.cdl.workflow.steps.rating;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.BaseCalcStatsStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;

@Component("calcRatingStats")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalcRatingStats extends BaseCalcStatsStep<ProcessRatingStepConfiguration> {

    @Override
    public void execute() {
        prepare();
        executeFullCalculation();
    }

    @Override
    protected List<ProfileParameters.Attribute> getDeclaredAttrs() {
        List<String> ratingAttrs = getRatingAttrs(getBaseTable());
        List<ProfileParameters.Attribute> pAttrs = ratingAttrs.stream().map(attr -> {
            CategoricalBucket catBkt = new CategoricalBucket();
            catBkt.setCategories(Arrays.asList("A", "B", "C", "D", "E", "F"));
            return new ProfileParameters.Attribute(attr, null, null, catBkt);
        }).collect(Collectors.toList());
        pAttrs.addAll(super.getDeclaredAttrs());
        return pAttrs;
    }

    private List<String> getRatingAttrs(Table pivotedTable) {
        List<ColumnMetadata> cms = pivotedTable.getColumnMetadata();
        if (CollectionUtils.isNotEmpty(cms)) {
            return cms.stream()
                    .filter(cm -> (StringUtils.isBlank(cm.getJavaClass()) || "String".equals(cm.getJavaClass()))
                            && cm.getAttrName().startsWith(RatingEngine.RATING_ENGINE_PREFIX + "_")) //
                    .map(ColumnMetadata::getAttrName) //
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected String getStatsTableCtxKey() {
        return null; // not support retry
    }

}
