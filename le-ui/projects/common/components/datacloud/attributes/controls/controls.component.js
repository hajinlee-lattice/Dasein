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
            vm.category = AttrConfigStore.getCategory();

            if (vm.section == 'enable') {
                vm.store.setLimit(vm.store.getUsageLimit(vm.overview, vm.params.section));
            } else {
                var tab = vm.overview.Selections.filter(function(tab) {
                    return tab.DisplayName == vm.category;
                })[0];

                vm.store.setLimit(tab.Limit);
            }
        };

        vm.save = function() {
            vm.isSaving = true;
            vm.store.saveConfig();
        };
    }
});