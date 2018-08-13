angular.module('common.navigation.back', [])
.service('BackStore', function() {
    this.backState = '';
    this.backLabel = '';
    this.hide = false;

    this.setBackState = function(state) {
        this.backState = state;
    }

    this.getBackState = function() {
        return this.backState;
    }

    this.setBackLabel = function(label) {
        this.backLabel = label;
    }

    this.getBackLabel = function() {
        return this.backLabel;
    }

    this.setHidden = function(hide) {
        this.hide = hide;
    }

    this.isHidden = function() {
        return this.hide;
    }
})
.component('backNav', {
    templateUrl: 'app/navigation/header/back/back.component.html',
    controller: function (BackStore) {
        var vm = this;
        vm.headerBack = '';
        vm.backName = '';
        vm.hide = false;
        this.$onInit = function() {
            vm.headerBack = BackStore.getBackLabel();
            vm.backName = BackStore.getBackState();
            vm.hide = BackStore.isHidden();
        };

        vm.getBackLabel = function() {
            return BackStore.getBackLabel();
        }

        vm.getBackState = function() {
            return BackStore.getBackState();
        }

        vm.isHidden = function() {
            return BackStore.isHidden();
        }
    }
});