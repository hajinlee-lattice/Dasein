angular.module('mainApp.appCommon.widgets.LeadsTabWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility'
])
.controller('LeadsTabWidgetController', function ($scope, $rootScope, $http, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
    var data = $scope.data;
})
.directive('leadsTabWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/leadsTabWidget/LeadsTabWidgetTemplate.html'
    };
    return directiveDefinitionObject;
});