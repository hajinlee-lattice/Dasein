angular.module('common.attributes.subheader', [])
.component('attrSubheader', {
    templateUrl: '/components/datacloud/attributes/subheader/subheader.component.html',
    bindings: {
        overview: '<'
    },
    controller: function ($state, $stateParams, AttrConfigStore, StateHistory) {
        var vm = this;

        vm.$onInit = function() {
            vm.section = AttrConfigStore.getSection();
            vm.tabs = AttrConfigStore.getTabMetadata(vm.section);

            if (vm.section == 'enable') {
                vm.overview = vm.overview.Selections;
            }

            console.log('init subheader', vm.section, vm.overview, vm.tabs);

            vm.tabs.forEach(function(item, index) {
                item.Selected = item.Selected ? item.Selected : 0;
                vm.tabs[index] = angular.extend({}, item, vm.overview[item.category]);
            });

            vm.params = $stateParams;
        };

        vm.click = function(name) {
            ShowSpinner('Loading Attribute Configuration');

            var params = {
                activate: {
                    category: name, 
                    subcategory: '' 
                },
                enable: {
                    section: name,
                    category: 'Intent', 
                    subcategory: '' 
                },
                edit: {
                    category: name, 
                    subcategory: '' 
                }
            };

            $state.go('.', params[vm.section]);
        };

        vm.getTo = function() {
            return StateHistory.lastToParams();
        };

        vm.getFrom = function() {
            return StateHistory.lastFromParams();
        };

        vm.isActive = function(tab) {
            var t_c = tab.category;
            var t_l = tab.label;
            var p_c = vm.params.category;
            var p_s = vm.params.section;
            var act = vm.section == 'activate';
            var key = act ? 'category' : 'section';
            var val = act ? t_c : t_l;

            var x = act ? t_c == p_c : t_l == p_s;
            var y = vm.getTo()[key] == val;
            var z = vm.getFrom()[key] != val || vm.getTo()[key] == val;

            return (x || y) && z;
        };
    }
});