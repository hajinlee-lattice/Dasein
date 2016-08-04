angular.module('lp.apiconsole.ScoringResponseController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.directives.charts.ArcChartDirective',
    'lp.apiconsole.APIConsoleService'
])
.directive('scoringResponse', function () {
    return {
        templateUrl: 'app/apiConsole/views/ScoringResponseView.html',
        controller: ['$scope', 'ResourceUtility', 'APIConsoleService',
            function ($scope, ResourceUtility, APIConsoleService) {
                $scope.ResourceUtility = ResourceUtility;
                $scope.ChartSize = 30;
                $scope.ChartTotal = 100;
                $scope.ChartColor = APIConsoleService.CalculateArcColor($scope.score);
            }
        ]
    };
});