angular.module('login.saml.metadata', [])
.component('loginSamlMetadata', {
    templateUrl: 'app/login/saml/metadata/metadata.component.html',
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