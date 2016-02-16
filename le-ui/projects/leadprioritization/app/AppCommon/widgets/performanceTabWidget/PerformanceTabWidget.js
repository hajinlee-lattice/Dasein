angular.module('mainApp.appCommon.widgets.PerformanceTabWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.appCommon.widgets.performanceTab.ThresholdExplorer',
    'mainApp.appCommon.widgets.performanceTab.LiftChart',
    'mainApp.appCommon.widgets.performanceTab.DataTable'
])
.directive('performanceTabWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/performanceTabWidget/PerformanceTabTemplate.html',
        controller: ['$scope', '$filter', 'ResourceUtility', function ($scope, $filter, ResourceUtility) {
            //if ($scope.data == null) return;

            $scope.threasholdData = $scope.data.ThresholdChartData;

            $scope.decileData = [[ ResourceUtility.getString("DECILE_GRID_CONVERSIONS") ]];
            $scope.data.ThresholdDecileData.forEach(function(d){
                $scope.decileData[0].push($filter("number")(d, 0) + "%");
            });
            var top_label = ResourceUtility.getString("DECILE_GRID_TOP_LABEL");
            $scope.decileColumns = [
                { title: ResourceUtility.getString("DECILE_GRID_LEADS_LABEL") },
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
            $scope.data.ThresholdLiftData.forEach(function(d){
                $scope.liftData[0].push($filter("number")(d, 1) + "x");
            });
            $scope.liftColumns = [
                { title: ResourceUtility.getString("LIFT_GRID_LEAD_SCORE") },
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