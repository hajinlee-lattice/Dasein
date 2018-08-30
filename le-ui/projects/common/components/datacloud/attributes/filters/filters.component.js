/* jshint -W014 */
angular.module('common.attributes.filters', [])
.component('attrFilters', {
    templateUrl: '/components/datacloud/attributes/filters/filters.component.html',
    bindings: {
        filters: '<'
    },
    controller: function (AttrConfigStore) {
        var vm = this;

        vm.$onInit = function() {
            vm.section = AttrConfigStore.getSection();
        };

        vm.isFilterSelected = function() {
            return vm.filters.show.Selected
                || vm.filters.hide.Selected
                || vm.filters.show.IsPremium
                || vm.filters.hide.IsPremium;
        };
    }
});