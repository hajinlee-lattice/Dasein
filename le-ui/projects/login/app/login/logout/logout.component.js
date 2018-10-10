angular.module('login')
.component('logoutView', {
    templateUrl: 'app/login/logout/logout.component.html',
    controller: function(
        $state, $location, $timeout, $interval, 
        ResourceUtility, LoginService, Banner, SessionTimeoutUtility, BrowserStorageUtility, LoginStore
    ) {
        var vm = this;

        angular.extend(vm, {
            redirectDelay: 10 // change to 0 to disable
        });

        var timer = function(seconds) {
            if(!seconds) {
                return false;
            }
            var redirectDelaySeconds = seconds,
                redirectDelayMs = redirectDelaySeconds * 1000,
                countDown = redirectDelaySeconds;

            $timeout(function() {
                $state.go("login.form");
            }, redirectDelayMs + 1000);

            $interval(function(){
                vm.delay = countDown--
            }, 1000, redirectDelaySeconds);
        }

        vm.$onInit = function() {
            timer(vm.redirectDelay);
        };

        vm.loginClick = function() {
            $state.go("login.form");
        }
    }
});