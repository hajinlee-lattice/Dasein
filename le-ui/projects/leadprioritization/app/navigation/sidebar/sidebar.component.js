angular
.module('pd.navigation.sidebar', [])
.controller('SidebarController', function($rootScope) {
    var vm = this;

    angular.extend(vm, {});

    vm.init = function() {
        if (typeof(sessionStorage) !== 'undefined') {
            if (sessionStorage.getItem('open-nav') === 'true' || !sessionStorage.getItem('open-nav')) {
                $("body").addClass('open-nav');
            } else {
                $("body").removeClass('open-nav');
            }
        }
    }

    vm.handleSidebarToggle = function($event) {
        var target = angular.element($event.target),
            collapsable_click = !target.parents('.menu').length;

        if (collapsable_click) {
            $('body').toggleClass('open-nav');
            $('body').addClass('controlled-nav');  // indicate the user toggled the nav

            if (typeof(sessionStorage) !== 'undefined'){
                sessionStorage.setItem('open-nav', $('body').hasClass('open-nav'));
            }

            $rootScope.$broadcast('sidebar:toggle');
        }
    }

    vm.init();
});