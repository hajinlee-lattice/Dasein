angular.module('login')
.component('logoutView', {
    templateUrl: 'app/login/logout/logout.component.html',
    controller: function(
        $state, $location, $timeout, ResourceUtility, LoginService, Banner,
        SessionTimeoutUtility, BrowserStorageUtility, LoginStore
    ) {
        var vm = this;

        vm.$onInit = function() {};

        vm.loginClick = function() {
            $state.go("login.form");
        }
    }
});