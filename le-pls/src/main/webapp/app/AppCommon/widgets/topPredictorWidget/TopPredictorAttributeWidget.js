angular.module('mainApp.appCommon.widgets.TopPredictorAttributeWidget', [
    'mainApp.appCommon.utilities.ResourceUtility'
])

.controller('TopPredictorAttributeWidgetController', function ($scope, ResourceUtility) {
    var chartData = $scope.data;


})

.directive('topPredictorAttributeWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/topPredictorWidget/TopPredictorAttributeWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});