angular
.module('mainApp.navigation.summary.OneLineController', [])
.controller('OneLineController', function($scope, ResourceUtility, ResourceString) {
    $scope.summaryTitle = ResourceUtility.getString(ResourceString);
});