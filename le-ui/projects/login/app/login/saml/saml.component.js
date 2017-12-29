angular.module('login.saml', [])
.component('loginSaml', {
    templateUrl: 'app/login/saml/saml.component.html',
    controller: function (
        $scope, $state, $location
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
        };

        vm.init();
    }
});