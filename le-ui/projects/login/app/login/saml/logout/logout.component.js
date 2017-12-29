angular.module('login.saml.logout', [])
.component('loginSamlLogout', {
    templateUrl: 'app/login/saml/logout/logout.component.html',
    controller: function (
        $scope, $state
    ) {
        var vm = this,
            resolve = $scope.$parent.$resolve;

        angular.extend(vm, {
        });

        vm.init = function() {
            console.log('saml logout');
        };

        vm.init();
    }
});