angular.module('login.saml', [])
.component('loginSaml', {
    templateUrl: 'app/login/saml/saml.component.html',
    controller: function (
        $scope, $state, $location, $rootScope,
        LoginService, LoginStore
    ) {
        var vm = this,
            resolve = $scope.$parent.$resolve;

        angular.extend(vm, {
            tenantId: $state.params.tenantId,
            userDocument: makeUserDocument($location.search().userDocument)
        });

        function makeUserDocument(rawUserDocument) {
            return JSON.parse(rawUserDocument);
        }

        vm.init = function() {
            LoginService.SamlLogin(vm.userDocument).then(function(result) {
                $state.go('login.tenants');
            });
        };

        vm.init();
    }
});