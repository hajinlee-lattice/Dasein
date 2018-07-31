angular.module('login.saml.error', [])
.component('loginSamlError', {
    templateUrl: 'app/login/saml/error/error.component.html',
    controller: function (ResourceUtility, $state) {
        var vm = this;

        vm.$onInit = function() {
        	vm.ResourceUtility = ResourceUtility;
        	vm.dateString = '2010 - ' + (new Date()).getFullYear();
        	console.log($state);          
        };
    }
});