angular.module('mainApp.appCommon.widgets.PerformanceTabWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.performanceTab.ThresholdExplorer',
    'mainApp.appCommon.widgets.performanceTab.LiftChart',
    'mainApp.appCommon.widgets.performanceTab.DataTable'
])
.directive('performanceTabWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/performanceTabWidget/PerformanceTabTemplate.html',
        controller: ['$scope', '$filter', 'ResourceUtility', 'RatingsEngineStore', function ($scope, $filter, ResourceUtility, RatingsEngineStore) {
            //if ($scope.data == null) return;

            var ratingEngine = RatingsEngineStore.getCurrentRating(),
                expectedValueModel = ratingEngine.latest_iteration.AI.predictionType == "EXPECTED_VALUE" ? true : false;

            $scope.conversionsOrRevenue = expectedValueModel ? 'Revenue' : 'Conversions';
            $scope.threasholdData = $scope.data.ThresholdChartData;
            $scope.sourceSchemaType = $scope.data.ModelDetails.SourceSchemaInterpretation;

            $scope.decileData = [[ "% Total " + $scope.conversionsOrRevenue ]];
            $scope.data.ThresholdDecileData.forEach(function(d){
                $scope.decileData[0].push($filter("number")(d, 0) + "%");
            });
            var top_label = ResourceUtility.getString("DECILE_GRID_TOP_LABEL");
            $scope.decileColumns = [
                { title: $scope.sourceSchemaType == "SalesforceLead" ? ResourceUtility.getString("DECILE_GRID_LEADS_LABEL")
                    : ResourceUtility.getString("DECILE_GRID_ACCOUNTS_LABEL")},
                { title: "10%", superTitle: top_label },
                { title: "20%", superTitle: top_label },
                { title: "30%", superTitle: top_label },
                { title: "40%", superTitle: top_label },
                { title: "50%", superTitle: top_label },
                { title: "60%", superTitle: top_label },
                { title: "70%", superTitle: top_label },
                { title: "80%", superTitle: top_label },
                { title: "90%", superTitle: top_label },
                { title: "100%", superTitle: top_label }
            ];

            $scope.liftChartData = $scope.data.ThresholdLiftData;

            $scope.liftData = [[ ResourceUtility.getString("LIFT_GRID_LIFT") ]];
            ($scope.data.ThresholdLiftData || []).forEach(function(d){
                $scope.liftData[0].push($filter("number")(d, 1) + "x");
            });
            $scope.liftColumns = [
                { title: $scope.sourceSchemaType == "SalesforceLead" ? ResourceUtility.getString("LIFT_GRID_LEAD_SCORE")
                    : ResourceUtility.getString("LIFT_GRID_ACCOUNT_SCORE")},
                { title: "91-100" },
                { title: "81-90" },
                { title: "71-80" },
                { title: "61-70" },
                { title: "51-60" },
                { title: "41-50" },
                { title: "31-40" },
                { title: "21-30" },
                { title: "11-20" },
                { title: "1-10" }
            ];
        }]
    };
});