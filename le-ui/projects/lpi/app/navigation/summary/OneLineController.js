angular
.module('lp.navigation.oneline', [])
.controller('OneLineController', function($scope, ResourceUtility, ResourceString) {
    $scope.summaryTitle = ResourceUtility.getString(ResourceString);
});