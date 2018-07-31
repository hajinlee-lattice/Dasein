angular.module('login.saml.logout', [])
.component('loginSamlLogout', {
    templateUrl: 'app/login/saml/logout/logout.component.html',
    controller: function (ResourceUtility, $state) {
        var vm = this;

        vm.$onInit = function() {
        	vm.ResourceUtility = ResourceUtility;
        	vm.dateString = '2010 - ' + (new Date()).getFullYear();
        };
    }
});