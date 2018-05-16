/* jshint -W014 */
angular.module('common.attributes.filters', [])
.component('attrFilters', {
    templateUrl: '/components/datacloud/attributes/filters/filters.component.html',
    bindings: {
        overview: '<',
        config: '<',
        filters: '< '
    },
    controller: function ($state, $stateParams, AttrConfigStore) {
        var vm = this;

        vm.$onInit = function() {
            console.log('init attrFilters', vm);
            vm.section = AttrConfigStore.getSection();
        };

        vm.isFilterSelected = function() {
            return vm.filters.showSelected
                || vm.filters.hideSelected
                || vm.filters.showPremium
                || vm.filters.hidePremium;
        };
    }
});