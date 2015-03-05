angular.module('mainApp.appCommon.widgets.LeadsTabWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility'
])
.controller('LeadsTabWidgetController', function ($scope, $rootScope, $http, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
})
.directive('leadsTabWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/leadsTabWidget/LeadsTabWidgetTemplate.html'
    };
    return directiveDefinitionObject;
})
.directive('leadsTable', function() {
    return {
        restrict:    'E',
        templateUrl: 'app/AppCommon/widgets/leadsTabWidget/LeadsTableTemplate.html',
        scope:       {leads: '=', title: '@'},
        controller:  ['$scope', '$attrs', '$http', 'ResourceUtility', function ($scope, $attrs, $http, ResourceUtility) {
            $scope.ResourceUtility = ResourceUtility;
        }]
    };
});