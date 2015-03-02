angular.module('mainApp.appCommon.widgets.AdminInfoWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.appCommon.utilities.StringUtility'
])
.controller('AdminInfoWidgetController', function ($scope, $rootScope, ResourceUtility, DateTimeFormatUtility, GriotNavUtility, StringUtility) {
    $scope.ResourceUtility = ResourceUtility;
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    var parentData = $scope.parentData;

})
.directive('adminInfoWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/adminInfoWidget/AdminInfoWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});