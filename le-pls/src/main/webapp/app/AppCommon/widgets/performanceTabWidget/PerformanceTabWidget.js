angular.module('mainApp.appCommon.widgets.PerformanceTabWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.appCommon.widgets.performanceTab.ThresholdExplorer',
    'mainApp.appCommon.widgets.performanceTab.DataTable'
])

.directive('performanceTabWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/performanceTabWidget/PerformanceTabTemplate.html',
        controller: ['$scope', '_', 'ResourceUtility', function ($scope, _, ResourceUtility) {
            if ($scope.data == null) return;

            $scope.threasholdData = $scope.data.ThresholdChartData;
            $scope.decileData = [[ ResourceUtility.getString("DECILE_GRID_CONVERSIONS") ]];
            $scope.data.ThresholdDecileData.forEach(function(d){
                $scope.decileData[0].push(d.toFixed(0));
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
        }]
    };
});