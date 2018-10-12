angular.module('login')
.component('logoutView', {
    templateUrl: 'app/login/logout/logout.component.html',
    controller: function(
        $scope, $state, $location, $timeout, $interval, 
        ResourceUtility, LoginService, Banner, SessionTimeoutUtility, BrowserStorageUtility, LoginStore
    ) {
        var vm = this,
            redirectDelayInterval,
            redirectDelayTimeout;

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

            redirectDelayTimeout = $timeout(function() {
                $state.go('login.form', { obj: $location.$$search });
            }, redirectDelayMs + 1000);

            redirectDelayInterval = $interval(function(){
                vm.delay = countDown--
                console.log(vm.delay);
            }, 1000, redirectDelaySeconds);

            $scope.$on('$destroy', function() {
                $interval.cancel(redirectDelayInterval);
                $timeout.cancel(redirectDelayTimeout);
            });
        }

        vm.$onInit = function() {
            timer(vm.redirectDelay);
        };

        vm.loginClick = function() {
            $state.go('login.form', { obj: $location.$$search });
        }
    }
});