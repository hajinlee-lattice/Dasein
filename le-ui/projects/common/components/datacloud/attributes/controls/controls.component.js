/* jshint -W014 */
angular.module('common.attributes.controls', [])
.component('attrControls', {
    templateUrl: '/components/datacloud/attributes/controls/controls.component.html',
    bindings: {
        overview: '<'
    },
    controller: function ($stateParams, AttrConfigStore, Modal) {
        var vm = this;

        vm.store = AttrConfigStore;

        vm.$onInit = function() {
            vm.params = $stateParams;
            vm.section = vm.store.getSection();
            vm.data = vm.store.get('data');
            vm.filters = vm.store.get('filters');
            vm.category = vm.store.get('category');

            if (vm.section == 'enable') {
                vm.store.set('limit', vm.store.getUsageLimit(vm.overview, vm.params.section));
            } else {
                var tab = vm.overview.Selections.filter(function(tab) {
                    return tab.DisplayName == vm.category;
                })[0];

                vm.store.set('limit', tab.Limit);
            }
        };

        vm.save = function() {
            var payload = vm.store.generatePayload();
            
            if (vm.section == 'activate' && payload.Select.length > 0) {
                Modal.warning({
                    name: 'AttrAdmin_Warning',
                    title: "Activation",
                    message: "Once you activate these premium attributes, you won't be able to deactivate.  Contact your Lattice representative to upgrade your data licenses.",
                    confirmtext: "Yes, activate"
                }, vm.store.modalCallback);
            } else {
                vm.store.saveConfig();
            }
        };

        vm.getTotalAttrs = function() {
            return vm.store.get('TotalFilteredAttrs');
        };
    }
});