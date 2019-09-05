package com.latticeengines.domain.exposed.dataflow;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.Transient;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CdlPivotScoreAndEventParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CreateCdlEventTableFilterParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CreateCdlEventTableParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlAccountParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlMergeParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlSplitParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.OrphanContactExportParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.OrphanTransactionExportParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.UnmatchedAccountExportParameters;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.CascadingBulkMatchDataflowParameters;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.CreateReportParameters;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.ParseMatchResultParameters;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.PrepareMatchDataParameters;
import com.latticeengines.domain.exposed.serviceflows.modeling.dataflow.DedupEventTableParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculatePredictedRevenuePercentileParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineFilterTableParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineMatchDebugWithScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.ComputeLiftParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.PivotScoreAndEventParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.RecalculateExpectedRevenueParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.RecalculatePercentileScoreParameters;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = CombineFilterTableParameters.class, name = "combineFilterTableParameters"), //
        @JsonSubTypes.Type(value = CombineInputTableWithScoreParameters.class, name = "combineInputTableWithScoreParameters"), //
        @JsonSubTypes.Type(value = CombineMatchDebugWithScoreParameters.class, name = "combineMatchDebugWithScoreParameters"), //
        @JsonSubTypes.Type(value = DedupEventTableParameters.class, name = "dedupEventTableParameters"), //
        @JsonSubTypes.Type(value = AddStandardAttributesParameters.class, name = "addStandardAttributesParameters"), //
        @JsonSubTypes.Type(value = CreateCdlEventTableParameters.class, name = "createCdlEventTableParameters"), //
        @JsonSubTypes.Type(value = CreateCdlEventTableFilterParameters.class, name = "createCdlEventTableFilterParameters"), //
        @JsonSubTypes.Type(value = CreateReportParameters.class, name = "createReportParameters"), //
        @JsonSubTypes.Type(value = ParseMatchResultParameters.class, name = "parseMatchResultParameters"), //
        @JsonSubTypes.Type(value = CascadingBulkMatchDataflowParameters.class, name = "cascadingBulkMatchDataflowParameters"), //
        @JsonSubTypes.Type(value = PivotScoreAndEventParameters.class, name = "pivotScoreAndEventParameters"), //
        @JsonSubTypes.Type(value = CdlPivotScoreAndEventParameters.class, name = "cdlPivotScoreAndEventParameters"), //
        @JsonSubTypes.Type(value = RecalculatePercentileScoreParameters.class, name = "recalculatePercentileScoreParameters"), //
        @JsonSubTypes.Type(value = RecalculateExpectedRevenueParameters.class, name = "recalculateExpectedRevenueParameters"), //
        @JsonSubTypes.Type(value = CalculatePredictedRevenuePercentileParameters.class, name = "calculatePredictedRevenuePercentileParameters"), //
        @JsonSubTypes.Type(value = CalculateExpectedRevenuePercentileParameters.class, name = "calculateExpectedRevenuePercentileParameters"), //
        @JsonSubTypes.Type(value = ComputeLiftParameters.class, name = "computeLiftParameters"), //
        @JsonSubTypes.Type(value = MatchCdlAccountParameters.class, name = "matchCdlAccountParameters"), //
        @JsonSubTypes.Type(value = MatchCdlSplitParameters.class, name = "MatchCdlSplitParameters"), //
        @JsonSubTypes.Type(value = MatchCdlMergeParameters.class, name = "MatchCdlMergeParameters"), //
        @JsonSubTypes.Type(value = PrepareMatchDataParameters.class, name = "PrepareMatchDataParameters"), //
        @JsonSubTypes.Type(value = OrphanTransactionExportParameters.class, name = "OrphanTransactionExportParameters"), //
        @JsonSubTypes.Type(value = OrphanContactExportParameters.class, name = "OrphanContactExportParameters"), //
        @JsonSubTypes.Type(value = UnmatchedAccountExportParameters.class, name = "UnmatchedAccountExportParameters"), //
})
public class DataFlowParameters {

    private static final Logger log = LoggerFactory.getLogger(DataFlowParameters.class);

    @JsonProperty("no_flink")
    public boolean noFlink = false;

    @Transient
    @JsonIgnore
    public final Set<String> getSourceTableNames() {
        List<Field> fields = FieldUtils.getFieldsListWithAnnotation(getClass(),
                SourceTableName.class);

        Set<String> sources = new HashSet<>();
        for (Field field : fields) {
            try {
                field.setAccessible(true);
                Object value = field.get(this);
                if (value != null) {
                    sources.add(value.toString());
                }
            } catch (IllegalAccessException e) {
                log.warn("Not able to add source for field " + field.getName());
                // pass
            }
        }
        return sources;
    }
}
