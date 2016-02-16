angular.module('mainApp.appCommon.widgets.AdminInfoAlertsWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.ModelAlertsService',
    'mainApp.core.services.SessionService',
    'mainApp.models.services.ModelService'
])
.controller('AdminInfoAlertsWidgetController', function ($scope, $rootScope, $http, ResourceUtility, ModelAlertsService, ModelStore) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.data = ModelStore.data;
    $scope.modelAlerts = $scope.data.ModelAlerts;
    $scope.suppressedCategories = $scope.data.SuppressedCategories;

    console.log('### adminAlerts', $scope.modelAlerts, $scope.suppressedCategories, $scope.data);
    if ($scope.modelAlerts == null && $scope.suppressedCategories == null) {
        $scope.showErrorMessage = true;
        $scope.alertTabErrorMessage = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_ERROR_MESSAGE");
    } else {
        $scope.showErrorMessage = false;
        $scope.warnings = ModelAlertsService.GetWarnings($scope.modelAlerts, $scope.suppressedCategories);
    }
})
.directive('adminInfoAlertsWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/adminInfoAlertsWidget/AdminInfoAlertsWidgetTemplate.html'
    };
});