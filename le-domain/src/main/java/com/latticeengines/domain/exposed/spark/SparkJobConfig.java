package com.latticeengines.domain.exposed.spark;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.spark.CreateCdlEventTableJobConfig;
import com.latticeengines.domain.exposed.serviceflows.core.spark.ParseMatchResultJobConfig;
import com.latticeengines.domain.exposed.serviceflows.core.spark.PrepareMatchDataJobConfig;
import com.latticeengines.domain.exposed.serviceflows.core.spark.ScoreAggregateJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CombineInputTableWithScoreJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.PivotScoreAndEventJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.AccountContactExportConfig;
import com.latticeengines.domain.exposed.spark.cdl.AggDailyActivityConfig;
import com.latticeengines.domain.exposed.spark.cdl.AppendRawStreamConfig;
import com.latticeengines.domain.exposed.spark.cdl.CalculateDeltaJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.CountOrphanTransactionsConfig;
import com.latticeengines.domain.exposed.spark.cdl.CreateDeltaRecommendationConfig;
import com.latticeengines.domain.exposed.spark.cdl.CreateEventTableFilterJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.CreateRecommendationConfig;
import com.latticeengines.domain.exposed.spark.cdl.DailyStoreToPeriodStoresJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.DeriveActivityMetricGroupJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.GenerateAccountLookupConfig;
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchArtifactsJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.LegacyDeleteJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeActivityMetricsJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeCSVConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeScoringTargetsConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeSystemBatchConfig;
import com.latticeengines.domain.exposed.spark.cdl.PivotRatingsConfig;
import com.latticeengines.domain.exposed.spark.cdl.ProcessDimensionConfig;
import com.latticeengines.domain.exposed.spark.cdl.RemoveOrphanConfig;
import com.latticeengines.domain.exposed.spark.cdl.SelectByColumnConfig;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.domain.exposed.spark.common.CountAvroGlobsConfig;
import com.latticeengines.domain.exposed.spark.common.MultiCopyConfig;
import com.latticeengines.domain.exposed.spark.common.UpsertConfig;
import com.latticeengines.domain.exposed.spark.dcp.SplitImportMatchResultConfig;
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig;

import reactor.core.publisher.Flux;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "Name")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = ScriptJobConfig.class, name = ScriptJobConfig.NAME), //
        @JsonSubTypes.Type(value = RemoveOrphanConfig.class, name = RemoveOrphanConfig.NAME), //
        @JsonSubTypes.Type(value = SoftDeleteConfig.class, name = SoftDeleteConfig.NAME), //
        @JsonSubTypes.Type(value = CreateDeltaRecommendationConfig.class, name = CreateDeltaRecommendationConfig.NAME), //
        @JsonSubTypes.Type(value = CreateRecommendationConfig.class, name = CreateRecommendationConfig.NAME), //
        @JsonSubTypes.Type(value = MergeImportsConfig.class, name = MergeImportsConfig.NAME), //
        @JsonSubTypes.Type(value = MergeScoringTargetsConfig.class, name = MergeScoringTargetsConfig.NAME), //
        @JsonSubTypes.Type(value = UpsertConfig.class, name = UpsertConfig.NAME), //
        @JsonSubTypes.Type(value = CopyConfig.class, name = CopyConfig.NAME), //
        @JsonSubTypes.Type(value = MultiCopyConfig.class, name = MultiCopyConfig.NAME), //
        @JsonSubTypes.Type(value = ConvertToCSVConfig.class, name = ConvertToCSVConfig.NAME), //
        @JsonSubTypes.Type(value = PivotRatingsConfig.class, name = PivotRatingsConfig.NAME), //
        @JsonSubTypes.Type(value = TestJoinJobConfig.class, name = TestJoinJobConfig.NAME), //
        @JsonSubTypes.Type(value = PrepareMatchDataJobConfig.class, name = PrepareMatchDataJobConfig.NAME), //
        @JsonSubTypes.Type(value = ParseMatchResultJobConfig.class, name = ParseMatchResultJobConfig.NAME), //
        @JsonSubTypes.Type(value = ScoreAggregateJobConfig.class, name = ScoreAggregateJobConfig.NAME), //
        @JsonSubTypes.Type(value = CombineInputTableWithScoreJobConfig.class, name = CombineInputTableWithScoreJobConfig.NAME), //
        @JsonSubTypes.Type(value = CreateCdlEventTableJobConfig.class, name = CreateCdlEventTableJobConfig.NAME), //
        @JsonSubTypes.Type(value = CreateEventTableFilterJobConfig.class, name = CreateEventTableFilterJobConfig.NAME), //
        @JsonSubTypes.Type(value = MergeSystemBatchConfig.class, name = MergeSystemBatchConfig.NAME), //
        @JsonSubTypes.Type(value = PivotScoreAndEventJobConfig.class, name = PivotScoreAndEventJobConfig.NAME), //
        @JsonSubTypes.Type(value = CountAvroGlobsConfig.class, name = CountAvroGlobsConfig.NAME), //
        @JsonSubTypes.Type(value = CalculateDeltaJobConfig.class, name = CalculateDeltaJobConfig.NAME), //
        @JsonSubTypes.Type(value = GenerateLaunchArtifactsJobConfig.class, name = GenerateLaunchArtifactsJobConfig.NAME), //
        @JsonSubTypes.Type(value = TestPartitionJobConfig.class, name = TestPartitionJobConfig.NAME), //
        @JsonSubTypes.Type(value = AccountContactExportConfig.class, name = AccountContactExportConfig.NAME), //
        @JsonSubTypes.Type(value = AppendRawStreamConfig.class, name = AppendRawStreamConfig.NAME), //
        @JsonSubTypes.Type(value = DailyStoreToPeriodStoresJobConfig.class, name = DailyStoreToPeriodStoresJobConfig.NAME), //
        @JsonSubTypes.Type(value = DeriveActivityMetricGroupJobConfig.class, name = DeriveActivityMetricGroupJobConfig.NAME), //
        @JsonSubTypes.Type(value = DailyStoreToPeriodStoresJobConfig.class, name = DailyStoreToPeriodStoresJobConfig.NAME), //
        @JsonSubTypes.Type(value = ProcessDimensionConfig.class, name = ProcessDimensionConfig.NAME), //
        @JsonSubTypes.Type(value = AggDailyActivityConfig.class, name = AggDailyActivityConfig.NAME), //
        @JsonSubTypes.Type(value = GenerateAccountLookupConfig.class, name = GenerateAccountLookupConfig.NAME), //
        @JsonSubTypes.Type(value = MergeActivityMetricsJobConfig.class, name = MergeActivityMetricsJobConfig.NAME),
        @JsonSubTypes.Type(value = CountOrphanTransactionsConfig.class, name = CountOrphanTransactionsConfig.NAME),
        @JsonSubTypes.Type(value = LegacyDeleteJobConfig.class, name = LegacyDeleteJobConfig.NAME),
        @JsonSubTypes.Type(value = SelectByColumnConfig.class, name = SelectByColumnConfig.NAME), //
        @JsonSubTypes.Type(value = MergeCSVConfig.class, name = MergeCSVConfig.NAME), //
        @JsonSubTypes.Type(value = SplitImportMatchResultConfig.class, name = SplitImportMatchResultConfig.NAME), //
        @JsonSubTypes.Type(value = ProfileJobConfig.class, name = ProfileJobConfig.NAME)
})
public abstract class SparkJobConfig implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 6562316419718067155L;

    @JsonProperty("Input")
    private List<DataUnit> input;

    @JsonProperty("Workspace")
    private String workspace;

    @JsonProperty("SpecialTargets")
    private Map<Integer, DataUnit.DataFormat> specialTargets;

    public abstract String getName();

    public int getNumTargets() {
        return 1;
    }

    public void setSpecialTargets(Map<Integer, DataUnit.DataFormat> specialTargets) {
        this.specialTargets = new HashMap<>(specialTargets);
    }

    public Map<Integer, DataUnit.DataFormat> getSpecialTargets() {
        return specialTargets;
    }

    public void setSpecialTarget(int idx, DataUnit.DataFormat dataFormat) {
        if (dataFormat == null) {
            return;
        }
        if (specialTargets == null) {
            specialTargets = new HashMap<>();
        }
        specialTargets.put(idx, dataFormat);
    }

    public String getWorkspace() {
        return workspace;
    }

    public void setWorkspace(String workspace) {
        this.workspace = workspace;
    }

    public List<DataUnit> getInput() {
        return input;
    }

    public void setInput(List<DataUnit> input) {
        this.input = input;
    }

    @JsonIgnore
    public List<HdfsDataUnit> getTargets() {
        if (getNumTargets() > 0) {
            Map<Integer, DataUnit.DataFormat> specialFmts = getSpecialTargets();
            String root;
            if (workspace.endsWith("/")) {
                root = workspace.substring(0, workspace.lastIndexOf("/"));
            } else {
                root = workspace;
            }
            return Flux.range(0, getNumTargets()).map(idx -> {
                HdfsDataUnit dataUnit = HdfsDataUnit.fromPath(root + "/Output" + (idx + 1));
                if (specialFmts != null && specialFmts.containsKey(idx)) {
                    dataUnit.setDataFormat(specialFmts.get(idx));
                }
                return dataUnit;
            }).collectList().block();
        } else {
            return Collections.emptyList();
        }
    }

}
