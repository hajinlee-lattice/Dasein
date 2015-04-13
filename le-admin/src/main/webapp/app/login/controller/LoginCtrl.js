var app = angular.module("app.login.controller.LoginCtrl", []);

app.controller('LoginCtrl', function($scope, $state){
    $scope.onLoginClick = function(){
        $state.go('TENANT.LIST');
    };
});
