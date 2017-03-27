angular
.module('common.datacloud.explorer.filters', [])
.directive('explorerFilters',function() {
    return {
        restrict: 'EA',
        scope: {
            vm:'='
        },
        templateUrl: '/components/datacloud/explorer/filters/filters.component.html',
        controller: function ($scope, $document, $state, $timeout, $interval, DataCloudStore) {
            var vm = $scope.vm;

            angular.extend(vm, {
                orders: {
                    attributeLookupMode: [ '-Value', 'DisplayName'],
                    attribute: ['-HighlightHighlighted', 'DisplayName'],
                    subcategory: 'toString()',
                    category: 'toString()'
                },
                download_button: {
                    class: 'orange-button select-label',
                    icon: 'fa fa-download',
                    iconclass: 'white-button select-more',
                    iconrotate: false,
                    tooltip: 'Download Enrichments'
                },
                sortPrefix: '+',
                view: 'list',
                queryText: ''
            });

            var clearFilters = function() {
                for(var i in vm.metadata.toggle) {
                    for(var j in vm.metadata.toggle[i]) {
                        vm.metadata.toggle[i][j] = '';
                    }
                }
            }

            vm.init_filters = function() {
                vm.download_button.items = [{
                    href: '/files/latticeinsights/insights/downloadcsv?onlySelectedAttributes=false&Authorization=' + vm.authToken,
                    label: vm.label.button_download,
                    icon: 'fa fa-file-o'
                },{
                    href: '/files/latticeinsights/insights/downloadcsv?onlySelectedAttributes=true&Authorization=' + vm.authToken,
                    label: vm.label.button_download_selected,
                    icon: 'fa fa-file-o'
                }];

                clearFilters();

                $scope.$watchGroup([
                        'vm.metadata.toggle.show.nulls',
                        'vm.metadata.toggle.show.selected',
                        'vm.metadata.toggle.hide.selected',
                        'vm.metadata.toggle.show.premium',
                        'vm.metadata.toggle.hide.premium',
                        'vm.metadata.toggle.show.internal',
                        'vm.metadata.toggle.show.enabled',
                        'vm.metadata.toggle.hide.enabled',
                        'vm.metadata.toggle.show.highlighted',
                        'vm.metadata.toggle.hide.highlighted'
                    ], function(newValues, oldValues, scope) {

                    vm.filterEmptySubcategories();
                    vm.TileTableItems = {};
                });

                $scope.$watchGroup([
                        'vm.premiumSelectedTotal',
                        'vm.generalSelectedTotal'
                    ], function(newValues, oldValues, scope) {

                    DataCloudStore.setMetadata('generalSelectedTotal', vm.generalSelectedTotal);
                    DataCloudStore.setMetadata('premiumSelectedTotal', vm.premiumSelectedTotal);
                });

                $scope.$watch('vm.queryText', function(newvalue, oldvalue){
                    vm.queryInProgress = true;

                    if (vm.queryTimeout) {
                        $timeout.cancel(vm.queryTimeout);
                    }

                    // debounce timeout to speed things up
                    vm.queryTimeout = $timeout(function() {
                        if (!vm.category && newvalue) {
                            vm.setCategory(vm.categories[0]);
                            vm.updateStateParams();
                        }

                        vm.query = vm.queryText;

                        if (vm.section != 'browse') {
                            vm.updateStateParams();
                        }

                        // maybe this will fix the issues where they dont trill down??
                        $timeout(function() {
                            var categories = Object.keys(vm.categoryCounts).filter(function(value, index) {
                                return vm.categoryCounts[value] > 0;
                            });

                            if (categories.length == 1 && !vm.lookupMode) {
                                vm.setCategory(categories[0]);
                                vm.filterEmptySubcategories();
                            }

                            vm.queryInProgress = false;
                        }, 1);

                        vm.filterEmptySubcategories();
                        vm.TileTableItems = {};
                    }, 500);
                });

                var find_dropdown_buttons = $interval(dropdown_buttons, 300),
                    find_dropdown_buttons_count = 0;

                function dropdown_buttons() {
                    var buttons = angular.element('.dropdown-container > h2');
                    find_dropdown_buttons_count++;
                    if(buttons.length > 0 || find_dropdown_buttons_count > 5) {
                        $interval.cancel(find_dropdown_buttons);
                    }
                    buttons.click(function(e){
                        var button = angular.element(this),
                            toggle_on = !button.hasClass('active'),
                            parent = button.closest('.dropdown-container');

                        parent.removeClass('active');
                        buttons.removeClass('selected');
                        buttons.parents().find('.dropdown-container').removeClass('active');
                        buttons.siblings('ul.dropdown').removeClass('open');

                        if(toggle_on) {
                            parent.addClass('active');
                            button.addClass('active');
                            button.siblings('ul.dropdown').addClass('open');
                        }

                        e.stopPropagation();

                    });
                }

                angular.element(document).click(function(event) {
                    var target = angular.element(event.target),
                        el = angular.element('.dropdown-container ul.dropdown, button ul.button-dropdown, .button ul.button-dropdown'),
                        has_parent = target.parents().is('.dropdown-container'),
                        parent = el.parents().find('.dropdown-container'),
                        is_visible = el.is(':visible');

                    if(!has_parent) {
                        vm.closeHighlighterButtons();
                        el.removeClass('open');
                        parent.removeClass('active');
                        el.siblings('.button.active').removeClass('active');
                    }
                    if(is_visible && !has_parent) {
                        $scope.$digest(); //ben -- hrmmm, works for now
                    }

                });

                DataCloudStore.setMetadata('premiumSelectLimit', (vm.EnrichmentPremiumSelectMaximum.data && vm.EnrichmentPremiumSelectMaximum.data['HGData_Pivoted_Source']) || 10);
                DataCloudStore.setMetadata('generalSelectLimit', 100);
                vm.premiumSelectLimit = DataCloudStore.getMetadata('premiumSelectLimit'); //(vm.EnrichmentPremiumSelectMaximum.data && vm.EnrichmentPremiumSelectMaximum.data['HGData_Pivoted_Source']) || 10;
                vm.generalSelectLimit = DataCloudStore.getMetadata('generalSelectLimit');

                if(vm.show_internal_filter) {
                    /*
                     * this is the default for the internal filter
                     * this also effectivly hides internal attributes when the filter is hidden
                    */
                    vm.metadata.toggle.show.internal = true;
                } else {
                    vm.metadata.toggle.show.internal = false;
                }

                if(vm.section === 'insights') {
                    /* hide disabled for sales team from iframe */
                    vm.metadata.toggle.show.enabled = true;
                } else {
                    vm.metadata.toggle.show.enabled = '';
                }
            }

            vm.sortOrder = function() {
                var sortPrefix = vm.sortPrefix.replace('+','');
                if (!vm.category) {
                    return handleFilterOrder(vm.orders.category);
                } else if (vm.subcategories[vm.category] && vm.subcategories[vm.category].length && !vm.subcategory) {
                    return handleFilterOrder(vm.orders.subcategory);
                } else {
                    if (vm.lookupMode && vm.category == 'Technology Profile' || vm.category == 'Website Profile') {
                        return handleFilterOrder(vm.orders.attributeLookupMode);
                    } else {
                        return handleFilterOrder(vm.orders.attribute);
                    }
                }
            }

            var handleFilterOrder = function(order, sortPrefix) {
                var sortPrefix = sortPrefix || vm.sortPrefix.replace('+','');
                if(typeof order === 'object') {
                    var sortArr = order,
                        retArr = [];

                    sortArr.forEach(function(item, index) {
                        retArr[index] = (item == 'DisplayName' ? sortPrefix : '') + item;
                    });

                    return retArr;
                }
                return sortPrefix + order;
            }

            var textSearch = function(haystack, needle, case_insensitive) {
                var case_insensitive = (case_insensitive === false ? false : true);

                if (case_insensitive) {
                    var haystack = haystack.toLowerCase(),
                    needle = needle.toLowerCase();
                }

                // .indexOf is faster and more supported than .includes
                return (haystack.indexOf(needle) >= 0);
            }

            vm.searchFields = function(enrichment){
                if (vm.query) {
                    if (textSearch(enrichment.DisplayName, vm.query)) {
                        return true;
                    } else if (textSearch(enrichment.Description, vm.query)) {
                        return true;
                    } else {
                        return false;
                    }
                }

                return true;
            }

            vm.enrichmentsFilter = function() {
                var filter = {};

                if (vm.metadata.toggle.show.selected && !vm.metadata.toggle.hide.selected) {
                    filter.IsSelected = true;
                }

                if (!vm.metadata.toggle.show.selected && vm.metadata.toggle.hide.selected) {
                    filter.IsSelected = false;
                }

                if (vm.metadata.toggle.show.premium && !vm.metadata.toggle.hide.premium) {
                    filter.IsPremium = true;
                }

                if (!vm.metadata.toggle.show.premium && vm.metadata.toggle.hide.premium) {
                    filter.IsPremium = false;
                }

                if (!vm.metadata.toggle.show.internal) {
                    filter.IsInternal = false;
                }

                if (vm.subcategory) {
                    filter.Subcategory = vm.subcategory;
                }

                if (vm.section == 'team' || vm.section == 'insights') {
                    filter.HighlightHidden = (!vm.metadata.toggle.hide.enabled ? '' : true) || (!vm.metadata.toggle.show.enabled ? '' : false);
                    filter.HighlightHighlighted = (!vm.metadata.toggle.show.highlighted ? '' : true) || (!vm.metadata.toggle.hide.highlighted ? '' : false);
                }

                if (vm.lookupMode && vm.isYesNoCategory(vm.category)) {
                    filter.AttributeValue = (!vm.metadata.toggle.show.nulls ? '!' + 'No' : '');
                }

                return filter;
            }

            vm.subcategoryFilter = function(subcategory) {
                if(!vm.enrichments_completed) {
                    return true;
                }
                var category = vm.category,
                    count = vm.subcategoryCount(category, subcategory);

                return (count ? true : false);
            }

            vm.refineQuery = function() {
                $state.go('home.model.analysis.explorer.query');
            }

            vm.init_filters();
        }
    };
});