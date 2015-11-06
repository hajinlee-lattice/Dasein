angular.module('mainApp.appCommon.widgets.AdminInfoAlertsWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.ModelAlertsService',
    'mainApp.core.services.SessionService'
])
.controller('AdminInfoAlertsWidgetController', function ($scope, $rootScope, $http, ResourceUtility, ModelAlertsService) {
    $scope.ResourceUtility = ResourceUtility;
    var modelAlerts = $scope.data.ModelAlerts;
    var suppressedCategories = $scope.data.SuppressedCategories;
    if (modelAlerts == null && suppressedCategories == null) {
        $scope.showErrorMessage = true;
        $scope.alertTabErrorMessage = ResourceUtility.getString("ADMIN_INFO_ALERTS_PAGE_ERROR_MESSAGE");
    } else {
        $scope.showErrorMessage = false;
        $scope.warnings = ModelAlertsService.GetWarnings(modelAlerts, suppressedCategories);
    }
})
.directive('adminInfoAlertsWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/adminInfoAlertsWidget/AdminInfoAlertsWidgetTemplate.html'
    };
});