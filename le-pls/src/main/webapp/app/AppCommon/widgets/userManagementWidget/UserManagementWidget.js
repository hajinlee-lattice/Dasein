angular.module('mainApp.appCommon.widgets.ModelDetailsWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.appCommon.utilities.StringUtility'
])
.controller('UserManagementWidgetController', function ($scope, $rootScope, ResourceUtility, DateTimeFormatUtility, GriotNavUtility, StringUtility) {
    $scope.ResourceUtility = ResourceUtility;

    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;

    $scope.showAddUserButton = metadata.CanAddUser;

})
.directive('userManagementWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/userManagementWidget/UserManagementWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});