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
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculatePredictedRevenuePercentileJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CombineInputTableWithScoreJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.PivotScoreAndEventJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.RecalculateExpectedRevenueJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.RecalculatePercentileScoreJobConfig;
import com.latticeengines.domain.exposed.spark.am.MapAttributeTxfmrConfig;
import com.latticeengines.domain.exposed.spark.cdl.AccountContactExportConfig;
import com.latticeengines.domain.exposed.spark.cdl.ActivityAlertJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.AggDailyActivityConfig;
import com.latticeengines.domain.exposed.spark.cdl.AppendRawStreamConfig;
import com.latticeengines.domain.exposed.spark.cdl.CalculateDeltaJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.CalculateLastActivityDateConfig;
import com.latticeengines.domain.exposed.spark.cdl.ConcatenateAIRatingsConfig;
import com.latticeengines.domain.exposed.spark.cdl.CountOrphanTransactionsConfig;
import com.latticeengines.domain.exposed.spark.cdl.CountProductTypeConfig;
import com.latticeengines.domain.exposed.spark.cdl.CreateDeltaRecommendationConfig;
import com.latticeengines.domain.exposed.spark.cdl.CreateEventTableFilterJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.CreateRecommendationConfig;
import com.latticeengines.domain.exposed.spark.cdl.DailyStoreToPeriodStoresJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.DailyTxnStreamPostAggregationConfig;
import com.latticeengines.domain.exposed.spark.cdl.DeriveActivityMetricGroupJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.EnrichWebVisitJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.ExportToElasticSearchJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.ExtractListSegmentCSVConfig;
import com.latticeengines.domain.exposed.spark.cdl.GenerateAccountLookupConfig;
import com.latticeengines.domain.exposed.spark.cdl.GenerateCuratedAttributesConfig;
import com.latticeengines.domain.exposed.spark.cdl.GenerateIntentAlertArtifactsConfig;
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchArtifactsJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchUniverseJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.GenerateLiveRampLaunchArtifactsJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.GenerateRecommendationCSVConfig;
import com.latticeengines.domain.exposed.spark.cdl.GenerateTimelineExportArtifactsJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.JoinAccountStoresConfig;
import com.latticeengines.domain.exposed.spark.cdl.JourneyStageJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.LegacyDeleteJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeActivityMetricsJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeCSVConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeLatticeAccountConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeProductConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeScoringTargetsConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeSystemBatchConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeTimeSeriesDeleteDataConfig;
import com.latticeengines.domain.exposed.spark.cdl.MigrateActivityPartitionKeyJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.PeriodTxnStreamPostAggregationConfig;
import com.latticeengines.domain.exposed.spark.cdl.PivotRatingsConfig;
import com.latticeengines.domain.exposed.spark.cdl.ProcessDimensionConfig;
import com.latticeengines.domain.exposed.spark.cdl.PublishActivityAlertsJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.PublishTableToElasticSearchJobConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.PublishVIDataJobConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.RemoveOrphanConfig;
import com.latticeengines.domain.exposed.spark.cdl.SelectByColumnConfig;
import com.latticeengines.domain.exposed.spark.cdl.SoftDeleteConfig;
import com.latticeengines.domain.exposed.spark.cdl.SplitSystemBatchStoreConfig;
import com.latticeengines.domain.exposed.spark.cdl.SplitTransactionConfig;
import com.latticeengines.domain.exposed.spark.cdl.TimeLineJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.TransformTxnStreamConfig;
import com.latticeengines.domain.exposed.spark.cdl.TruncateLatticeAccountConfig;
import com.latticeengines.domain.exposed.spark.cdl.ValidateProductConfig;
import com.latticeengines.domain.exposed.spark.cm.CMTpsLookupCreationConfig;
import com.latticeengines.domain.exposed.spark.cm.CMTpsSourceCreationConfig;
import com.latticeengines.domain.exposed.spark.common.ApplyChangeListConfig;
import com.latticeengines.domain.exposed.spark.common.ChangeListConfig;
import com.latticeengines.domain.exposed.spark.common.ConvertMatchResultConfig;
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.domain.exposed.spark.common.CountAvroGlobsConfig;
import com.latticeengines.domain.exposed.spark.common.FilterByJoinConfig;
import com.latticeengines.domain.exposed.spark.common.FilterChangelistConfig;
import com.latticeengines.domain.exposed.spark.common.GenerateChangeTableConfig;
import com.latticeengines.domain.exposed.spark.common.GetColumnChangesConfig;
import com.latticeengines.domain.exposed.spark.common.GetRowChangesConfig;
import com.latticeengines.domain.exposed.spark.common.MultiCopyConfig;
import com.latticeengines.domain.exposed.spark.common.UpsertConfig;
import com.latticeengines.domain.exposed.spark.dcp.AnalyzeUsageConfig;
import com.latticeengines.domain.exposed.spark.dcp.InputPresenceConfig;
import com.latticeengines.domain.exposed.spark.dcp.PrepareDataReportConfig;
import com.latticeengines.domain.exposed.spark.dcp.RollupDataReportConfig;
import com.latticeengines.domain.exposed.spark.dcp.SplitImportMatchResultConfig;
import com.latticeengines.domain.exposed.spark.graph.AssignEntityIdsJobConfig;
import com.latticeengines.domain.exposed.spark.graph.ConvertAccountsToGraphJobConfig;
import com.latticeengines.domain.exposed.spark.graph.GraphPageRankJobConfig;
import com.latticeengines.domain.exposed.spark.graph.MergeGraphsJobConfig;
import com.latticeengines.domain.exposed.spark.stats.AdvancedCalcStatsConfig;
import com.latticeengines.domain.exposed.spark.stats.BucketEncodeConfig;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsConfig;
import com.latticeengines.domain.exposed.spark.stats.CalcStatsDeltaConfig;
import com.latticeengines.domain.exposed.spark.stats.FindChangedProfileConfig;
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig;
import com.latticeengines.domain.exposed.spark.stats.UpdateProfileConfig;

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
        @JsonSubTypes.Type(value = CalculateExpectedRevenuePercentileJobConfig.class, name = CalculateExpectedRevenuePercentileJobConfig.NAME), //
        @JsonSubTypes.Type(value = RecalculatePercentileScoreJobConfig.class, name = RecalculatePercentileScoreJobConfig.NAME), //
        @JsonSubTypes.Type(value = CalculatePredictedRevenuePercentileJobConfig.class, name = CalculatePredictedRevenuePercentileJobConfig.NAME), //
        @JsonSubTypes.Type(value = RecalculateExpectedRevenueJobConfig.class, name = RecalculateExpectedRevenueJobConfig.NAME), //
        @JsonSubTypes.Type(value = CreateCdlEventTableJobConfig.class, name = CreateCdlEventTableJobConfig.NAME), //
        @JsonSubTypes.Type(value = CreateEventTableFilterJobConfig.class, name = CreateEventTableFilterJobConfig.NAME), //
        @JsonSubTypes.Type(value = MergeSystemBatchConfig.class, name = MergeSystemBatchConfig.NAME), //
        @JsonSubTypes.Type(value = ChangeListConfig.class, name = ChangeListConfig.NAME), //
        @JsonSubTypes.Type(value = ApplyChangeListConfig.class, name = ApplyChangeListConfig.NAME), //
        @JsonSubTypes.Type(value = PivotScoreAndEventJobConfig.class, name = PivotScoreAndEventJobConfig.NAME), //
        @JsonSubTypes.Type(value = CountAvroGlobsConfig.class, name = CountAvroGlobsConfig.NAME), //
        @JsonSubTypes.Type(value = CalculateDeltaJobConfig.class, name = CalculateDeltaJobConfig.NAME), //
        @JsonSubTypes.Type(value = GenerateLaunchArtifactsJobConfig.class, name = GenerateLaunchArtifactsJobConfig.NAME), //
        @JsonSubTypes.Type(value = GenerateLaunchUniverseJobConfig.class, name = GenerateLaunchUniverseJobConfig.NAME), //
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
        @JsonSubTypes.Type(value = AnalyzeUsageConfig.class, name = AnalyzeUsageConfig.NAME), //
        @JsonSubTypes.Type(value = SplitImportMatchResultConfig.class, name = SplitImportMatchResultConfig.NAME), //
        @JsonSubTypes.Type(value = SplitSystemBatchStoreConfig.class, name = SplitSystemBatchStoreConfig.NAME), //
        @JsonSubTypes.Type(value = ProfileJobConfig.class, name = ProfileJobConfig.NAME), //
        @JsonSubTypes.Type(value = UpdateProfileConfig.class, name = UpdateProfileConfig.NAME), //
        @JsonSubTypes.Type(value = FindChangedProfileConfig.class, name = FindChangedProfileConfig.NAME), //
        @JsonSubTypes.Type(value = GenerateCuratedAttributesConfig.class, name = GenerateCuratedAttributesConfig.NAME), //
        @JsonSubTypes.Type(value = CalculateLastActivityDateConfig.class, name = CalculateLastActivityDateConfig.NAME), //
        @JsonSubTypes.Type(value = BucketEncodeConfig.class, name = BucketEncodeConfig.NAME), //
        @JsonSubTypes.Type(value = CalcStatsConfig.class, name = CalcStatsConfig.NAME), //
        @JsonSubTypes.Type(value = CalcStatsDeltaConfig.class, name = CalcStatsDeltaConfig.NAME), //
        @JsonSubTypes.Type(value = AdvancedCalcStatsConfig.class, name = AdvancedCalcStatsConfig.NAME), //
        @JsonSubTypes.Type(value = MergeProductConfig.class, name = MergeProductConfig.NAME), //
        @JsonSubTypes.Type(value = ConcatenateAIRatingsConfig.class, name = ConcatenateAIRatingsConfig.NAME), //
        @JsonSubTypes.Type(value = MergeTimeSeriesDeleteDataConfig.class, name = MergeTimeSeriesDeleteDataConfig.NAME), //
        @JsonSubTypes.Type(value = TimeLineJobConfig.class, name = TimeLineJobConfig.NAME), //
        @JsonSubTypes.Type(value = JourneyStageJobConfig.class, name = JourneyStageJobConfig.NAME), //
        @JsonSubTypes.Type(value = ActivityAlertJobConfig.class, name = ActivityAlertJobConfig.NAME), //
        @JsonSubTypes.Type(value = ValidateProductConfig.class, name = ValidateProductConfig.NAME), //
        @JsonSubTypes.Type(value = InputPresenceConfig.class, name = InputPresenceConfig.NAME),
        @JsonSubTypes.Type(value = MergeLatticeAccountConfig.class, name = MergeLatticeAccountConfig.NAME), //
        @JsonSubTypes.Type(value = TruncateLatticeAccountConfig.class, name = TruncateLatticeAccountConfig.NAME), //
        @JsonSubTypes.Type(value = JoinAccountStoresConfig.class, name = JoinAccountStoresConfig.NAME), //
        @JsonSubTypes.Type(value = FilterChangelistConfig.class, name = FilterChangelistConfig.NAME), //
        @JsonSubTypes.Type(value = GetColumnChangesConfig.class, name = GetColumnChangesConfig.NAME), //
        @JsonSubTypes.Type(value = GetRowChangesConfig.class, name = GetRowChangesConfig.NAME), //
        @JsonSubTypes.Type(value = MapAttributeTxfmrConfig.class, name = MapAttributeTxfmrConfig.NAME), //
        @JsonSubTypes.Type(value = FilterByJoinConfig.class, name = FilterByJoinConfig.NAME), //
        @JsonSubTypes.Type(value = MigrateActivityPartitionKeyJobConfig.class, name = MigrateActivityPartitionKeyJobConfig.NAME), //
        @JsonSubTypes.Type(value = RollupDataReportConfig.class, name = RollupDataReportConfig.NAME), //
        @JsonSubTypes.Type(value = PrepareDataReportConfig.class, name = PrepareDataReportConfig.NAME), //
        @JsonSubTypes.Type(value = CMTpsSourceCreationConfig.class, name = CMTpsSourceCreationConfig.NAME), //
        @JsonSubTypes.Type(value = CMTpsLookupCreationConfig.class, name = CMTpsLookupCreationConfig.NAME), //
        @JsonSubTypes.Type(value = SplitTransactionConfig.class, name = SplitTransactionConfig.NAME), //
        @JsonSubTypes.Type(value = TransformTxnStreamConfig.class, name = TransformTxnStreamConfig.NAME), //
        @JsonSubTypes.Type(value = CountProductTypeConfig.class, name = CountProductTypeConfig.NAME), //
        @JsonSubTypes.Type(value = ExportToElasticSearchJobConfig.class, name = ExportToElasticSearchJobConfig.NAME),
        @JsonSubTypes.Type(value = CountProductTypeConfig.class, name = CountProductTypeConfig.NAME), //
        @JsonSubTypes.Type(value = GenerateTimelineExportArtifactsJobConfig.class, name = GenerateTimelineExportArtifactsJobConfig.NAME), //
        @JsonSubTypes.Type(value = CountProductTypeConfig.class, name = CountProductTypeConfig.NAME), //
        @JsonSubTypes.Type(value = PublishActivityAlertsJobConfig.class, name = PublishActivityAlertsJobConfig.NAME), //
        @JsonSubTypes.Type(value = GenerateRecommendationCSVConfig.class, name = GenerateRecommendationCSVConfig.NAME), //
        @JsonSubTypes.Type(value = GenerateIntentAlertArtifactsConfig.class, name = GenerateIntentAlertArtifactsConfig.NAME), //
        @JsonSubTypes.Type(value = ExtractListSegmentCSVConfig.class, name = ExtractListSegmentCSVConfig.NAME), //
        @JsonSubTypes.Type(value = GenerateLiveRampLaunchArtifactsJobConfig.class, name = GenerateLiveRampLaunchArtifactsJobConfig.NAME), //
        @JsonSubTypes.Type(value = PublishVIDataJobConfiguration.class, name = PublishVIDataJobConfiguration.NAME), //
        @JsonSubTypes.Type(value = DailyTxnStreamPostAggregationConfig.class, name = DailyTxnStreamPostAggregationConfig.NAME), //
        @JsonSubTypes.Type(value = PeriodTxnStreamPostAggregationConfig.class, name =
                PeriodTxnStreamPostAggregationConfig.NAME), //
        @JsonSubTypes.Type(value = EnrichWebVisitJobConfig.class, name = EnrichWebVisitJobConfig.NAME), //
        @JsonSubTypes.Type(value = PublishTableToElasticSearchJobConfiguration.class, name =
                PublishTableToElasticSearchJobConfiguration.NAME), //
        @JsonSubTypes.Type(value = ConvertMatchResultConfig.class, name = ConvertMatchResultConfig.NAME), //
        @JsonSubTypes.Type(value = GenerateChangeTableConfig.class, name = GenerateChangeTableConfig.NAME), //
        @JsonSubTypes.Type(value = GraphPageRankJobConfig.class, name = GraphPageRankJobConfig.NAME), //
        @JsonSubTypes.Type(value = ConvertAccountsToGraphJobConfig.class, name = ConvertAccountsToGraphJobConfig.NAME), //
        @JsonSubTypes.Type(value = MergeGraphsJobConfig.class, name = MergeGraphsJobConfig.NAME), //
        @JsonSubTypes.Type(value = AssignEntityIdsJobConfig.class, name = AssignEntityIdsJobConfig.NAME)
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

    //if partition number over than this value, will cut 1/2 partition num
    @JsonProperty("NumPartitionLimit")
    public Integer numPartitionLimit;

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
            return SparkConfigUtils.getTargetUnits(workspace, getSpecialTargets(), getNumTargets());
        } else {
            return Collections.emptyList();
        }
    }
}
