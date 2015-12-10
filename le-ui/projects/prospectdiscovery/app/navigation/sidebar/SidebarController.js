angular.module('pd.navigation.sidebar', [

])

.controller('SidebarCtrl', function($scope, $rootScope) {
    $scope.toggle = function() {
        $("body").toggleClass("open-nav");
    }
});