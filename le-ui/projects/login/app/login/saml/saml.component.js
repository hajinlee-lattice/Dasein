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
            return (rawUserDocument ? JSON.parse(rawUserDocument) : []);
        }

        vm.init = function() {
            LoginService.SamlLogin(vm.userDocument).then(function(result) {
                /**
                 * This takes the user to the tenants page, which is correct behavior: see below
                 * If there's one tenant login.tenants will automatically go to that tenant.  
                 * If there's more than one tenant login.tenants will show a list of those tenants to select from.
                 */
                $state.go('login.tenants');  
            });
        };

        vm.init();
    }
});