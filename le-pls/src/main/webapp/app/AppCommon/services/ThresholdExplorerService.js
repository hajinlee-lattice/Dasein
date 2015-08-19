angular.module('mainApp.appCommon.services.ThresholdExplorerService', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('ThresholdExplorerService', function (ResourceUtility) {

    this.PrepareData = function (modelSummary) {

        if (modelSummary == null ||
            modelSummary.ModelDetails == null ||
            modelSummary.Segmentations == null) {
            return {"ChartData": [], "DecileData": []};
        }

        var chartData = this.GetChartData(modelSummary);
        var decileData = this.GetDecileData(chartData);

        return {"ChartData" : chartData, "DecileData": decileData};
    };

    this.GetChartData = function (modelSummary) {
        var conversion;
        var i;
        //==================================================
        // Prepare
        //==================================================
        var totalLeads = modelSummary.ModelDetails.TestingLeads;
        var totalConversions = modelSummary.ModelDetails.TestingConversions;
        var avgConversion = totalConversions / totalLeads;

        var segments = modelSummary.Segmentations[0].Segments;

        var percentLeads = []; for (i = 0; i < 101; i++) percentLeads.push(i);

        var cumConversions = []; cumConversions.push(0);
        var cumCount = []; cumCount.push(0);
        for (i = 1; i < 101; i++) {
            cumConversions.push(cumConversions[i - 1] + segments[i - 1].Converted);
            cumCount.push(cumCount[i - 1] + segments[i - 1].Count);
        }

        var cumPctConversions = []; cumPctConversions.push(0);
        for (i = 1; i < 101; i++) {
            cumPctConversions.push(100 * (cumConversions[i] / totalConversions));
        }

        var leftLift = []; leftLift.push(0);
        for (i = 1; i < 101; i++) {
            conversion = cumConversions[i] / cumCount[i];
            leftLift.push(conversion / avgConversion);
        }

        var rightLift = [];
        for (i = 0; i < 100; i++) {
            conversion = (totalConversions - cumConversions[i]) / (totalLeads - cumCount[i]);
            rightLift.push(conversion / avgConversion);
        }
        rightLift.push(0);

        var score = []; score.push(0);
        for (i = 100; i > 0; i--) {
            score.push(i);
        }

        //==================================================
        // Load
        //==================================================
        var data = [];
        for (i = 0; i < 101; i++) {
            data.push({
                "leads": percentLeads[i],
                "score": score[i],
                "conversions": cumPctConversions[i],
                "leftLift": leftLift[i],
                "rightLift": rightLift[i]});
        }

        return data;
    };

    this.GetDecileData = function (chartData) {
        var result = [];
        for (var i = 1; i < 11; i++) {
            result.push(chartData[i * 10].conversions);
        }


        return result;
    };

    this.PrepareExportData = function (modelSummary) {
        var result = [];

        var chartData = modelSummary.hasOwnProperty("ThresholdChartData") ?
                        modelSummary.ThresholdChartData :
                        this.GetChartData(modelSummary);

        var segments = modelSummary.Segmentations[0].Segments;

        var columns = [
            ResourceUtility.getString("MODEL_ADMIN_THRESHOLD_EXPORT_SCORE_LABEL"),
            ResourceUtility.getString("MODEL_ADMIN_THRESHOLD_EXPORT_LEADS_LABEL"),
            ResourceUtility.getString("MODEL_ADMIN_THRESHOLD_EXPORT_CONVERSIONS_LABEL"),
            ResourceUtility.getString("MODEL_ADMIN_THRESHOLD_EXPORT_LEFT_LIFT_LABEL"),
            ResourceUtility.getString("MODEL_ADMIN_THRESHOLD_EXPORT_RIGHT_LIFT_LABEL"),
            ResourceUtility.getString("MODEL_ADMIN_THRESHOLD_EXPORT_COUNT_LABEL"),
            ResourceUtility.getString("MODEL_ADMIN_THRESHOLD_EXPORT_CONVERTED_LABEL")
        ];
        result.push(columns);

        for (var i = 1; i < 101; i++) {
            var row = [];
            row.push(chartData[i].score);
            row.push(chartData[i].leads + "%");
            row.push(chartData[i].conversions.toFixed(0) + "%");
            row.push(chartData[i].leftLift.toFixed(2));
            row.push(chartData[i].rightLift.toFixed(2));
            row.push(segments[i - 1].Count);
            row.push(segments[i - 1].Converted);
            result.push(row);
        }

        return result;
    };
});