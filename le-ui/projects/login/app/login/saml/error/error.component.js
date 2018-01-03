angular.module('login.saml.error', [])
.component('loginSamlMetadata', {
    templateUrl: 'app/login/saml/error/error.component.html',
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