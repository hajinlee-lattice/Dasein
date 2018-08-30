angular.module('common.attributes.subheader', [])
.component('attrSubheader', {
    templateUrl: '/components/datacloud/attributes/subheader/subheader.component.html',
    bindings: {
        overview: '<'
    },
    controller: function ($state, $stateParams, AttrConfigStore, StateHistory) {
        var vm = this;

        vm.store = AttrConfigStore;

        vm.$onInit = function() {
            vm.section = vm.store.getSection();
            vm.params = $stateParams;
            vm.tabs = vm.overview.Selections;
            
            switch(vm.section) {
                case "enable": vm.supplemental = "ENABLED"; break;
                case "enable": vm.supplemental = "ACTIVE"; break;
                default: vm.supplemental = ""; break;
            }

            vm.store.setData('overview', vm.overview);
        };

        vm.click = function(name) {
            ShowSpinner('Loading Attributes');

            var params = {
                activate: {
                    category: name, 
                    subcategory: '' 
                },
                enable: {
                    section: name,
                    category: '', 
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
            var t_c = tab.DisplayName;
            var t_l = tab.DisplayName;
            var p_c = vm.params.category;
            var p_s = vm.params.section;
            var act = vm.section == 'activate' || vm.section == 'edit';
            var key = act ? 'category' : 'section';
            var val = act ? t_c : t_l;

            var x = act ? t_c == p_c : t_l == p_s;
            var y = vm.getTo()[key] == val;
            var z = vm.getFrom()[key] != val || vm.getTo()[key] == val;

            return (x || y) && z;
        };
    }
});