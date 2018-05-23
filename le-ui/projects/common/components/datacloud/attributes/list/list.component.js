/* jshint -W014 */
angular.module('common.attributes.list', [])
.component('attrResultsList', {
    templateUrl: '/components/datacloud/attributes/list/list.component.html',
    bindings: {
        filters: '<'
    },
    controller: function ($state, $stateParams, AttrConfigStore) {
        var vm = this;

        vm.store = AttrConfigStore;
        vm.attributes = {};
        vm.indeterminate = {};
        vm.startChecked = {};
        vm.allCheckedMap = {};
        vm.allChecked = false;

        vm.$onInit = function() {
            console.log('attrResultsList', vm.data);
            vm.data = vm.store.getData();
            vm.section = vm.store.getSection();
            vm.params = $stateParams;

            vm.parseData();
            vm.countSelected();

            vm.store.setData('original', JSON.parse(JSON.stringify(vm.data.config)));
        };

        vm.parseData = function() {
            var total = [];

            vm.data.config.Subcategories.forEach(function(item) {
                var selected = item.Attributes.filter(function(attr) {
                    return attr.Selected;
                });

                selected.forEach(function(attr) {
                    vm.startChecked[attr.Attribute] = true;
                });
                
                item.checked = selected.length;
                item.Selected = vm.isChecked(item);

                vm.allCheckedMap[item.DisplayName] = item.checked == item.TotalAttrs;
                vm.attributes[item.DisplayName] = item.Attributes;

                total.concat(selected);
            });

            vm.store.setSelected(total);
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

            vm.store.setSelected(total);
            vm.data.config.Selected = vm.store.getSelected().length;
        };

        vm.getResults = function() {
            return vm.subcategory 
                ? vm.attributes[vm.subcategory] 
                : vm.data.config.Subcategories;
        };

        vm.getCount = function() {
            return vm.subcategory 
                ? vm.getSubcategory(vm.subcategory).TotalAttrs 
                : vm.data.config.TotalAttrs;
        };

        vm.getSubcategory = function(name) {
            return vm.data.config.Subcategories.filter(function(item) {
                return item.DisplayName == name;
            })[0];
        };

        vm.back = function() {
            vm.subcategory = '';
            vm.filters.page = 1;

            $state.go('.', { 
                subcategory: vm.subcategory 
            });
        };

        vm.click = function(item) {
            if (item.Attributes) {
                vm.subcategory = item.DisplayName;
                vm.filters.page = 1;

                $state.go('.', { 
                    subcategory: vm.subcategory 
                });
            } else {
                vm.toggleSelected(item);
            }
        };

        vm.isChecked = function(item) {
            if (item.Attributes) {
                if (vm.indeterminate[item.DisplayName] === true) {
                    vm.setIndeterminate(item.DisplayName, true);

                    item.Selected = true;
                    
                    return true;
                } else {
                    var condition = item.checked == item.TotalAttrs;
                    
                    item.Selected = condition;

                    return item.checked == item.TotalAttrs;
                }
            } else {
                return item.Selected;
            }
        };

        vm.isAllChecked = function() {
            var subcategory, selected, total, indeterminate;

            if (!vm.subcategory) {
                selected = vm.store.getSelected().length;
                total = vm.data.config.TotalAttrs;
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
            var hasFrozen = item.HasFrozenAttrs;
            var isFrozen = item.IsFrozen;
            var overLimit = false;

            if (vm.store.getLimit() >= 0) {
                overLimit = vm.store.getSelected().length >= vm.store.getLimit() && !vm.isChecked(item);
            }
            
            return item.Attributes ? overLimit : (isFrozen || overLimit);
        };

        vm.isStartsDisabled = function(item) {
            if (item.Attributes || vm.section == 'enable') {
                return false;
            }

            var startsDisabled = vm.startChecked[item.Attribute];

            return startsDisabled;
        };

        vm.toggleSelected = function(item) {
            if (vm.isDisabled(item) || vm.isStartsDisabled(item)) {
                return false;
            }

            if (item.Attributes) {
                vm.setIndeterminate(item.DisplayName, false);

                item.Attributes
                    .sort(vm.sortAttributes)
                    .forEach(function(attr) {
                        if (vm.isDisabled(attr) || vm.isStartsDisabled(attr)) {
                            return;
                        }

                        attr.Selected = (item.checked != item.TotalAttrs);

                        if (attr.Selected) {
                            vm.store.getSelected().push(attr);
                        }
                    });
            } else {
                item.Selected = !item.Selected;
            }

            vm.countSelected();
        };

        vm.toggleAll = function() {
            if (vm.subcategory) {
                vm.allCheckedMap[vm.subcategory] = !vm.allCheckedMap[vm.subcategory];

                vm.attributes[vm.subcategory]
                    .sort(vm.sortAttributes)
                    .forEach(function(attr) {
                        if (vm.isDisabled(attr) || vm.isStartsDisabled(attr)) {
                            return;
                        }

                        attr.Selected = vm.allCheckedMap[vm.subcategory];

                        if (attr.Selected) {
                            vm.store.getSelected().push(attr);
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
                                if (vm.isDisabled(attr) || vm.isStartsDisabled(attr)) {
                                    return;
                                }

                                attr.Selected = vm.allChecked;

                                if (attr.Selected) {
                                    vm.store.getSelected().push(attr);
                                }
                            });
                    });
            }

            vm.countSelected();
        };

        vm.checkboxName = function() {
            return vm.subcategory ? 'check_all_' + vm.subcategory : 'all_attributes';
        };

        vm.setIndeterminate = function(checkbox, value) {
            vm.indeterminate[checkbox] = value;
            //$('[name="' + checkbox + '"]').prop('indeterminate', value);
        };

        vm.sortAttributes = function(a, b) {
            return a.Attribute.toLowerCase().localeCompare(b.Attribute.toLowerCase());
        };

        vm.sortSubcategories = function(a, b) {
            return a.toLowerCase().localeCompare(b.toLowerCase());
        };

        vm.getFiltering = function() {
            var obj = {};

            if (vm.filters.queryText) {
                obj.DisplayName = vm.filters.queryText;
            }

            Object.keys(vm.filters.show).forEach(function(property) {
                if (vm.filters.show[property] === true) {
                    obj[property] = true;
                }
            });

            Object.keys(vm.filters.hide).forEach(function(property) {
                if (vm.filters.hide[property] === true) {
                    obj[property] = false;
                }
            });

            //console.log(obj, vm.filters);

            return obj;
        };
    }
});