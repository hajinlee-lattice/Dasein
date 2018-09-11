angular.module('common.attributes.edit.list', [])
.component('attrEditList', {
    templateUrl: '/components/datacloud/attributes/edit/list/edit-list.component.html',
    bindings: {
        filters: '<'
    },
    controller: function ($state, $timeout, AttrConfigStore, AttrConfigService, DataCloudStore) {
        var vm = this;

        vm.store = AttrConfigStore;
        
        this.$onInit = function() {
            vm.accesslevel = vm.store.getAccessRestriction();
            vm.section = vm.store.getSection();
            vm.category = vm.store.get('category');
            vm.data = vm.store.get('data');

            vm.store.setData('original', JSON.parse(JSON.stringify(vm.data.config)));
        };
        
        this.getResults = function() {
            return this.data.config.Attributes;
        };

        this.onBlur = function(item, name) {
            if (item.DisplayName === '') {
                item.DisplayName = item.DefaultName;
            }

            var original = vm.store.getData('original')
                            .Attributes.filter(function(attr) {
                                return attr.Attribute == item.Attribute;
                            });

            original = original.length > 0 ? original[0] : item;

            if (item.DisplayName != original.DisplayName || item.Description != original.Description) {
                AttrConfigService.putConfig(
                    'name', vm.category, {}, { Attributes: [ item ] }
                ).then(function(result) {
                    angular.element('#'+name).addClass('saved');

                    $timeout(function() {
                        angular.element('#'+name).removeClass('saved');
                    }, 1250);

                    vm.attr_edit_form[name].$setPristine();

                    original.DisplayName = item.DisplayName;
                    original.Description = item.Description;

                    DataCloudStore.clear();
                });
            }
        };
    }
});