angular.module('common.attributes.edit.list', [])
.component('attrEditList', {
    templateUrl: '/components/datacloud/attributes/edit/list/list.component.html',
    bindings: {
        filters: '<'
    },
    controller: function ($state, AttrConfigStore, BrowserStorageUtility) {
        var vm = this;

        vm.store = AttrConfigStore;
        
        this.$onInit = function() {
            vm.accesslevel = vm.store.getAccessRestriction();
            vm.section = vm.store.getSection();
            vm.category = vm.store.get('category');
            vm.data = vm.store.get('data');
        };
        
        this.getResults = function() {
            return this.data.config.Attributes;
        };
    }
});