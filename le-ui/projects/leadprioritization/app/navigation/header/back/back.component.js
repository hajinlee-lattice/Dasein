angular.module('common.navigation.back', [])
.component('backNav', {
    templateUrl: 'app/navigation/header/back/back.component.html',
    bindings: {
        backconfig: '<'
    },
    controller: function () {
        var vm = this;
        vm.headerBack = '';
        vm.backName = '';
        vm.hide = false;
        this.$onInit = function() {
            vm.headerBack = this.backconfig.backState;
            vm.backName = this.backconfig.backName;
            vm.hide = this.backconfig.hide ? this.backconfig.hide : false;

        };
    }
});