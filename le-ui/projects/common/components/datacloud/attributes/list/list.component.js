/* jshint -W014 */
angular.module('common.attributes.list', [
    'common.attributes.subheader'
])
.component('attrResultsList', {
    templateUrl: '/components/datacloud/attributes/list/list.component.html',
    bindings: {
        overview: '<',
        config: '<',
        options: '< '
    },
    controller: function ($state, $stateParams, AttrConfigStore) {
        var vm = this;

        vm.page = 1;
        vm.pagesize = 25;
        vm.sortPrefix = '+';
        vm.selected = [];
        vm.attributes = {};
        vm.indeterminate = {};
        vm.startChecked = {};
        vm.allCheckedMap = {};
        vm.allChecked = false;

        vm.$onInit = function() {
            console.log('init attrList', vm);
            vm.section = AttrConfigStore.getSection();
            vm.params = $stateParams;
            vm.categories = vm.overview.AttrNums;

            if (vm.section == 'activate') {
                vm.limit = vm.overview[vm.params.category].Limit;
            }
            
            vm.parseData();
            vm.countSelected();

            vm.original = angular.copy(vm.config);
        };

        vm.parseData = function() {
            var total = [];

            vm.config.Subcategories.forEach(function(item) {
                var selected = item.Attributes.filter(function(attr) {
                    return attr.Selected;
                });

                selected.forEach(function(attr) {
                    vm.startChecked[attr.Attribute] = true;
                });
                
                item.checked = selected.length;
                item.Selected = vm.isChecked(item);

                vm.allCheckedMap[item.SubCategory] = item.checked == item.TotalAttrs;
                vm.attributes[item.SubCategory] = item.Attributes;

                total.concat(selected);
            });

            vm.selected = total;
        };

        vm.countSelected = function() {
            var total = [];

            Object.keys(vm.attributes).forEach(function(key) {
                var subcategory = vm.getSubcategory(key);
                var attributes = vm.attributes[key];
                var selected = [];

                attributes.forEach(function(attr, index) {
                    if (attr.Selected === true) {
                        selected.push(attr);
                        total.push(attr);
                    }
                });

                if (selected.length > 0 && selected.length != attributes.length) {
                    vm.indeterminate[key] = true;
                } else {
                    delete vm.indeterminate[key];
                }

                subcategory.checked = selected.length;
            });

            vm.selected = total;
            vm.config.Selected = vm.selected.length;
        };

        vm.getResults = function() {
            return vm.subcategory 
                ? vm.attributes[vm.subcategory] 
                : vm.config.Subcategories;
        };

        vm.getCount = function() {
            return vm.subcategory 
                ? vm.getSubcategory(vm.subcategory).TotalAttrs 
                : vm.config.TotalAttrs;
        };

        vm.getSubcategory = function(name) {
            return vm.config.Subcategories.filter(function(item) {
                return item.SubCategory == name;
            })[0];
        };

        vm.back = function() {
            vm.subcategory = '';
            vm.page = 1;

            $state.go('.', { 
                subcategory: vm.subcategory 
            });
        };

        vm.click = function(item) {
            if (item.Attributes) {
                vm.subcategory = item.SubCategory;
                vm.page = 1;

                $state.go('.', { 
                    subcategory: vm.subcategory 
                });
            } else {
                vm.checked(item);
            }
        };

        vm.checked = function(item) {
            if (vm.isDisabled(item)) {
                return false;
            }

            if (item.Attributes) {
                vm.setIndeterminate(item.SubCategory, false);

                item.Attributes
                    .sort(vm.sortAttributes)
                    .forEach(function(attr) {
                        if (vm.isDisabled(attr)) {
                            return;
                        }

                        attr.Selected = (item.checked != item.TotalAttrs);

                        if (attr.Selected) {
                            vm.selected.push(attr);
                        }
                    });
            } else {
                item.Selected = !item.Selected;
            }

            vm.countSelected();
        };

        vm.isChecked = function(item) {
            if (item.Attributes) {
                if (vm.indeterminate[item.SubCategory] === true) {
                    vm.setIndeterminate(item.SubCategory, true);
                } else {
                    return item.checked == item.TotalAttrs;
                }
            } else {
                return item.Selected;
            }
        };

        vm.isAllChecked = function() {
            var subcategory, selected, total, indeterminate;

            if (!vm.subcategory) {
                selected = vm.selected.length;
                total = vm.config.TotalAttrs;
            } else {
                subcategory = vm.getSubcategory(vm.subcategory);
                selected = subcategory.checked;
                total = subcategory.TotalAttrs;
            }

            indeterminate = (selected !== 0 && selected != total);
            vm.setIndeterminate(vm.checkboxName(), indeterminate);

            return selected !== 0;
        };

        vm.isDisabled = function(item) {
            if (vm.section == 'enable') {
                return false;
            }

            var overLimit = vm.selected.length >= vm.limit && !vm.isChecked(item);
            
            if (item.Attributes) {
                var hasFrozen = item.HasFrozenAttrs;
                return overLimit;
            } else {
                var startsDisabled = vm.startChecked[item.Attribute];
                var isFrozen = item.IsFrozen;
                return startsDisabled || isFrozen || overLimit;
            }
        };

        vm.toggleAll = function() {
            if (vm.subcategory) {
                vm.allCheckedMap[vm.subcategory] = !vm.allCheckedMap[vm.subcategory];

                vm.attributes[vm.subcategory]
                    .sort(vm.sortAttributes)
                    .forEach(function(attr) {
                        if (vm.isDisabled(attr)) {
                            return;
                        }

                        attr.Selected = vm.allCheckedMap[vm.subcategory];

                        if (attr.Selected) {
                            vm.selected.push(attr);
                        }
                    });
            } else {
                vm.allChecked = !vm.allChecked;

                Object.keys(vm.attributes)
                    .sort(vm.sortSubcategories)
                    .forEach(function(key) {
                        vm.setIndeterminate(key, false);
                        vm.attributes[key].checked = vm.attributes[key].TotalAttrs;
                        
                        vm.attributes[key]
                            .sort(vm.sortAttributes)
                            .forEach(function(attr) {
                                if (vm.isDisabled(attr)) {
                                    return;
                                }

                                attr.Selected = vm.allChecked;

                                if (attr.Selected) {
                                    vm.selected.push(attr);
                                }
                            });
                    });
            }

            vm.countSelected();
        };

        vm.checkboxName = function() {
            return vm.subcategory ? 'check_all_' + vm.subcategory : 'check_all';
        };

        vm.setIndeterminate = function(checkbox, value) {
            $('[name="' + checkbox + '"]').prop('indeterminate', value);
        };

        vm.isChanged = function() {
            var current = angular.copy(vm.config);
            return JSON.stringify(vm.original) === JSON.stringify(current);
        };

        vm.sortAttributes = function(a, b) {
            return a.Attribute.toLowerCase().localeCompare(b.Attribute.toLowerCase());
        };

        vm.sortSubcategories = function(a, b) {
            return a.toLowerCase().localeCompare(b.toLowerCase());
        };

        vm.save = function() {
            var activate = vm.section == 'activate';
            var type = activate ? 'activation' : 'usage';
            var category = vm.params.category;
            var usage = {};
            var data = {
                Select: []
            };

            if (!activate) {
                usage.usage = vm.params.section;
                data.Deselect = [];
            }
 
            vm.config.Subcategories.forEach(function(item) {
                item.Attributes.forEach(function(attr) {
                    if (attr.Selected) {
                        data.Select.push(attr.Attribute);
                    } else if (!activate) {
                        data.Deselect.push(attr.Attribute);
                    }
                });
            });
            
            AttrConfigStore.putConfig(type, category, usage, data).then(function() {
                console.log('save', vm.section, activate, type, category, usage, data);
                $state.reload();
            });
        };
    }
});