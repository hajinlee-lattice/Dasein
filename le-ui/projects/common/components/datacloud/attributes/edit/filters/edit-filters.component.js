angular.module('common.attributes.edit.filters', [])
.component('attrEditFilters', {
    templateUrl: '/components/datacloud/attributes/edit/filters/edit-filters.component.html',
    bindings: {
        filters: '<'
    },
    controller: function (AttrConfigStore) {
        var vm = this;

        vm.$onInit = function() {
            vm.section = AttrConfigStore.getSection();
        };
    }
});