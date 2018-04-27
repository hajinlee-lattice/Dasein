angular.module('login.saml', [])
.component('loginSaml', {
    templateUrl: 'app/login/saml/saml.component.html',
    controller: function ($state, $location, LoginService) {
        var vm = this;

        vm.$onInit = function() {
            vm.tenantId = $state.params.tenantId;
            vm.userDocument = makeUserDocument($location.search().userDocument);

            LoginService.SamlLogin(vm.userDocument).then(function(result) {
                /**
                 * This takes the user to the tenants page, which is correct behavior: see below
                 * If there's one tenant login.tenants will automatically go to that tenant.  
                 * If there's more than one tenant login.tenants will show a list of those tenants to select from.
                 */
                $state.go('login.tenants');
            });
        };

        function makeUserDocument(rawUserDocument) {
            return (rawUserDocument ? JSON.parse(rawUserDocument) : []);
        }
    }
});