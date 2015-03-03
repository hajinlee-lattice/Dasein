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

        chartData = this.GetChartData(modelSummary);
        decileData = this.GetDecileData(chartData);

        return {"ChartData" : chartData, "DecileData": decileData};
    };

    this.GetChartData = function (modelSummary) {
        //==================================================
        // Prepare
        //==================================================
        totalLeads = modelSummary.ModelDetails.TestingLeads;
        totalConversions = modelSummary.ModelDetails.TestingConversions;
        avgConversion = totalConversions / totalLeads;

        segments = modelSummary.Segmentations[0].Segments;

        percentLeads = []; for (i = 0; i < 101; i++) percentLeads.push(i);

        cumConversions = []; cumConversions.push(0);
        cumCount = []; cumCount.push(0);
        for (i = 1; i < 101; i++) {
            cumConversions.push(cumConversions[i - 1] + segments[i - 1].Converted);
            cumCount.push(cumCount[i - 1] + segments[i - 1].Count);
        }

        cumPctConversions = []; cumPctConversions.push(0);
        for (i = 1; i < 101; i++) {
            cumPctConversions.push(100 * (cumConversions[i] / totalConversions));
        }

        leftLift = []; leftLift.push(0);
        for (i = 1; i < 101; i++) {
            conversion = cumConversions[i] / cumCount[i];
            leftLift.push(conversion / avgConversion);
        }

        rightLift = [];
        for (i = 0; i < 100; i++) {
            conversion = (totalConversions - cumConversions[i]) / (totalLeads - cumCount[i]);
            rightLift.push(conversion / avgConversion);
        }
        rightLift.push(0);

        score = []; score.push(0);
        for (i = 100; i > 0; i--) {
            score.push(i);
        }

        //==================================================
        // Load
        //==================================================
        data = [];
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
        result = [];

        //==================================================
        // Conversions
        //==================================================
        var conversions = {Leads: ResourceUtility.getString("DECILE_GRID_CONVERSIONS")};
        for (i = 1; i < 11; i++) {
            conversions["D" + String(i)] = String(chartData[i * 10].conversions.toFixed(0)) + "%";
        }
        result.push(conversions);

        //==================================================
        // Lift
        //==================================================
        var leftLift = {Leads: ResourceUtility.getString("DECILE_GRID_LIFT")};
        for (i = 1; i < 11; i++) {
            leftLift["D" + String(i)] = String(chartData[i * 10].leftLift.toFixed(2)) + "x";
        }
        result.push(leftLift);

        return result;
    };
});