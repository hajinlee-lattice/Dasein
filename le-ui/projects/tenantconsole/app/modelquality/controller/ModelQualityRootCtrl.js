angular.module('app.modelquality', [
    'app.modelquality.service.InfluxDbService',
    'app.modelquality.service.ModelQualityService',
    'app.modelquality.controller.ModelQualityDashboardCtrl',
    'app.modelquality.controller.PipelineCtrl',
    'app.modelquality.controller.AnalyticPipelineCtrl',
    'app.modelquality.controller.AnalyticTestCtrl',
    'app.modelquality.controller.PublishLatestCtrl',
    'app.modelquality.directive.ModelQualityLineChart',
    'app.modelquality.directive.ModelQualityGroupBarChart'
])
.controller('ModelQualityRootCtrl', function ($scope, $state, $rootScope) {
    $scope.state = $state.current.name.split('.')[1];

    var stateChangeStart = $rootScope.$on('$stateChangeStart', function (event, toState, toParams, fromState, fromParams) {
        $scope.state = toState.name.split('.')[1];

        if (toState.name.indexOf('MODELQUALITY') === 0) {
            $scope.loading = true;
        }
    });

    var stateChangeSuccess = $rootScope.$on('$stateChangeSuccess', function(evt, toState, params) {
        $scope.loading = false;
    });

    $scope.$on('$destroy', function () {
        typeof stateChangeStart === 'function' ? stateChangeStart() : null;
        typeof stateChangeSuccess === 'function' ? stateChangeSuccess() : null;
    });

});
