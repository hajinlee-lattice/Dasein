/* jshint -W014 */
angular.module('common.attributes.controls', [])
.component('attrControls', {
    templateUrl: '/components/datacloud/attributes/controls/controls.component.html',
    bindings: {
        overview: '<'
    },
    controller: function ($state, $stateParams, AttrConfigStore) {
        var vm = this;

        vm.store = AttrConfigStore;
        vm.isSaving = false;

        vm.$onInit = function() {
            vm.section = vm.store.getSection();
            vm.data = vm.store.getData();
            vm.params = $stateParams;

            if (vm.section == 'enable') {
                vm.store.setLimit(vm.store.getUsageLimit(vm.overview, vm.params.section));
            } else {
                vm.store.setLimit(vm.overview[vm.params.category].Limit);
            }
        };

        vm.save = function() {
            vm.isSaving = true;
            vm.store.saveConfig();
        };
    }
});