angular.module('login.saml', [])
.component('loginSaml', {
    templateUrl: 'app/login/saml/saml.component.html',
    controller: function (
        $scope, $state
    ) {
        var vm = this,
            resolve = $scope.$parent.$resolve;

        angular.extend(vm, {
        });

        vm.init = function() {
        };

        vm.init();
    }
});