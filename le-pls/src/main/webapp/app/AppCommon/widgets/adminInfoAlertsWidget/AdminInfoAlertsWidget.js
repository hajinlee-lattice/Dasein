angular.module('mainApp.appCommon.widgets.AdminInfoAlertsWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.ModelAlertsService',
    'mainApp.core.services.SessionService'
])
.controller('AdminInfoAlertsWidgetController', function ($scope, $rootScope, $http, ResourceUtility, ModelAlertsService) {
    var modelAlerts = $scope.data.ModelAlerts;
    var suppressedCategories = $scope.data.SuppressedCategories;
    $scope.warnings = ModelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
})
.directive('adminInfoAlertsWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/adminInfoAlertsWidget/AdminInfoAlertsWidgetTemplate.html'
    };
});