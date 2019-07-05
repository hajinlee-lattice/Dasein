export default function () {
    return {
        restrict: 'EA',
        scope: {
            vm: '='
        },
        templateUrl: '/components/datacloud/explorer/filters/filters.component.html',
        controller: function (
            $scope, $state, $timeout, $interval, FeatureFlagService, QueryStore,
            DataCloudStore, SegmentStore, RatingsEngineStore, AuthorizationUtility
        ) {

            'ngInject';

            var vm = $scope.vm;

            angular.extend(vm, {
                orders: {
                    attributeLookupMode: ['-Value', 'DisplayName'],
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
                header: {
                    sort_modeliteration: {
                        label: 'Display Name',
                        icon: 'numeric',
                        property: 'DisplayName',
                        order: '',
                        items: [
                            { 
                                label: 'Predictive Power', 
                                icon: 'numeric', 
                                property: 'PredictivePower',
                                tooltip: 'Attributes with more Predictive Power are better at differentiating your higher and lower converting Accounts. Predictive Power is based on the mutual information between an Attribute and your Model Event.'
                            },
                            { label: 'Display Name', icon: 'alpha', property: 'DisplayName' },
                            { label: 'Feature Importance', icon: 'numeric', property: 'ImportanceOrdering' }
                        ]
                    },
                },
                sortPrefix: '+',
                view: 'list',
                queryText: '',
                QueryStore: QueryStore,
                saved: false,
                hasDeleteAccessRights: ['segment.analysis'].indexOf(vm.section) != -1 ? AuthorizationUtility.checkAccessLevel(AuthorizationUtility.excludeExternalUser) : false
            });

            // remove highlighting
            if (!vm.showHighlighting()) {
                vm.orders.attribute = vm.orders.attribute.filter(function (item) {
                    return item != '-HighlightHighlighted' && item != 'HighlightHighlighted'
                });
            }

            var clearFilters = function () {
                for (var i in vm.metadata.toggle) {
                    for (var j in vm.metadata.toggle[i]) {
                        vm.metadata.toggle[i][j] = '';
                    }
                }
            }

            vm.init_filters = function () {
                vm.download_button.items = [{
                    href: '/files/latticeinsights/insights/downloadcsv?onlySelectedAttributes=false&Authorization=' + vm.authToken,
                    label: vm.label.button_download,
                    icon: 'fa fa-file-o'
                }, {
                    href: '/files/latticeinsights/insights/downloadcsv?onlySelectedAttributes=true&Authorization=' + vm.authToken,
                    label: vm.label.button_download_selected,
                    icon: 'fa fa-file-o'
                }];

                clearFilters();

                $scope.$watchGroup([
                    'vm.metadata.toggle.hide.highlighted',
                    'vm.metadata.toggle.hide.selected',
                    'vm.metadata.toggle.hide.premium',
                    'vm.metadata.toggle.hide.enabled',
                    'vm.metadata.toggle.show.nulls',
                    'vm.metadata.toggle.show.selected',
                    'vm.metadata.toggle.show.premium',
                    'vm.metadata.toggle.show.internal',
                    'vm.metadata.toggle.show.enabled',
                    'vm.metadata.toggle.show.highlighted',
                    'vm.metadata.toggle.show.selected_ratingsengine_attributes'
                ], function (newValues, oldValues, scope) {
                    vm.filterEmptySubcategories();
                    vm.TileTableItems = {};
                });

                $scope.$watchGroup([
                    'vm.premiumSelectedTotal',
                    'vm.generalSelectedTotal'
                ], function (newValues, oldValues, scope) {
                    DataCloudStore.setMetadata('generalSelectedTotal', vm.generalSelectedTotal);
                    DataCloudStore.setMetadata('premiumSelectedTotal', vm.premiumSelectedTotal);
                });

                $scope.$watch('vm.queryText', function (newvalue, oldvalue) {

                    vm.queryInProgress = true;

                    if (vm.queryTimeout) {
                        $timeout.cancel(vm.queryTimeout);
                    }

                    // debounce timeout to speed things up
                    vm.queryTimeout = $timeout(function () {
                        if (!vm.category && newvalue) {
                            vm.setCategory(vm.categories[0]);
                            vm.updateStateParams();
                        }

                        vm.query = vm.queryText;

                        if (vm.section != 'browse') {
                            vm.updateStateParams();
                        }

                        // maybe this will fix the issues where they dont drill down??
                        $timeout(function () {
                            var categories = Object.keys(vm.categoryCounts).filter(function (value, index) {
                                return vm.categoryCounts[value] > 0;
                            });

                            if (vm.section == 're.model_iteration') {
                                vm.setCategory(categories[0]);
                                vm.filterEmptySubcategories();
                            }

                            if (vm.category && (categories.indexOf(vm.category) < 0 || categories.length == 1)) {

                                vm.setCategory(categories[0]);
                                vm.filterEmptySubcategories();
                            }

                            vm.queryInProgress = false;
                        }, 100);

                        vm.filterEmptySubcategories();
                        vm.TileTableItems = {};
                    }, 666);
                });

                var find_dropdown_buttons = $interval(dropdown_buttons, 300),
                    find_dropdown_buttons_count = 0;

                function dropdown_buttons() {
                    var buttons = angular.element('.dropdown-container > h2');
                    find_dropdown_buttons_count++;
                    if (buttons.length > 0 || find_dropdown_buttons_count > 5) {
                        $interval.cancel(find_dropdown_buttons);
                    }
                    buttons.click(function (e) {
                        var button = angular.element(this),
                            toggle_on = !button.hasClass('active'),
                            parent = button.closest('.dropdown-container');

                        parent.removeClass('active');
                        buttons.removeClass('selected');
                        buttons.parents().find('.dropdown-container').removeClass('active');
                        buttons.siblings('ul.dropdown').removeClass('open');

                        if (toggle_on) {
                            parent.addClass('active');
                            button.addClass('active');
                            button.siblings('ul.dropdown').addClass('open');
                        }

                        e.stopPropagation();

                    });
                }

                angular.element(document).click(function (event) {
                    var target = angular.element(event.target),
                        el = angular.element('.dropdown-container ul.dropdown, button ul.button-dropdown, .button ul.button-dropdown'),
                        has_parent = target.parents().is('.dropdown-container'),
                        parent = el.parents().find('.dropdown-container'),
                        is_visible = el.is(':visible');

                    if (!has_parent) {
                        vm.closeHighlighterButtons();
                        el.removeClass('open');
                        parent.removeClass('active');
                        el.siblings('.button.active').removeClass('active');
                    }
                    if (is_visible && !has_parent) {
                        $scope.$digest(); //ben -- hrmmm, works for now
                    }

                });

                DataCloudStore.setMetadata('premiumSelectLimit', (vm.EnrichmentPremiumSelectMaximum.data && vm.EnrichmentPremiumSelectMaximum.data['HGData_Pivoted_Source']) || 10);
                DataCloudStore.setMetadata('generalSelectLimit', vm.EnrichmentSelectMaximum || 100);
                vm.premiumSelectLimit = DataCloudStore.getMetadata('premiumSelectLimit'); //(vm.EnrichmentPremiumSelectMaximum.data && vm.EnrichmentPremiumSelectMaximum.data['HGData_Pivoted_Source']) || 10;
                vm.generalSelectLimit = DataCloudStore.getMetadata('generalSelectLimit');

                if (vm.show_internal_filter) {
                    /*
                     * this is the default for the internal filter
                     * this also effectivly hides internal attributes when the filter is hidden
                    */
                    vm.metadata.toggle.show.internal = true;
                } else {
                    vm.metadata.toggle.show.internal = false;
                }

                if (vm.section === 'insights') {
                    /* hide disabled for sales team from iframe */
                    vm.metadata.toggle.show.enabled = true;
                } else {
                    vm.metadata.toggle.show.enabled = '';
                }
            };

            vm.hideMessage = function () {
                vm.saved = false;
            };

            vm.isFilterSelected = function () {
                return (vm.section !== 'insights' && vm.metadata.toggle.show.enabled) ||
                    vm.metadata.toggle.show.selected || vm.metadata.toggle.hide.selected ||
                    vm.metadata.toggle.show.premium || vm.metadata.toggle.hide.premium ||
                    vm.metadata.toggle.hide.enabled || vm.metadata.toggle.show.highlighted ||
                    vm.metadata.toggle.hide.highlighted || vm.metadata.toggle.show.nulls ||
                    vm.metadata.toggle.show.internal;
            };

            vm.sortOrder = function () {
                if (vm.section == 're.model_iteration') {
                    var sortPrefix = vm.header.sort_modeliteration.order.replace('+', '');
                } else {
                    var sortPrefix = vm.sortPrefix.replace('+', '');
                }
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
            };

            var handleFilterOrder = function (order, sortPrefix) {
                var sortPrefix = sortPrefix || vm.sortPrefix.replace('+', '');

                if (typeof order === 'object') {
                    var sortArr = order,
                        retArr = [];

                    if (vm.section == 're.model_iteration') {
                        var sortPrefix = vm.header.sort_modeliteration.order.replace('+', '');
                        var importance = sortPrefix + 'ImportanceOrdering';
                        var nullImportance = '!ImportanceOrdering';
                        var predictive = sortPrefix + 'PredictivePower';
                        var nullPredictive = '!PredictivePower';
                        var name = sortPrefix + 'DisplayName';

                        if (vm.header.sort_modeliteration.property == 'ImportanceOrdering') {
                            retArr.push(nullImportance, importance, '');
                        }
                        if (vm.header.sort_modeliteration.property == 'PredictivePower') {
                            retArr.push(nullPredictive, predictive, '');
                        }
                        if (vm.header.sort_modeliteration.property == 'DisplayName') {
                            retArr.push(name);
                        }
                    } else {
                        sortArr.forEach(function (item, index) {
                            retArr[index] = (item == 'DisplayName' ? sortPrefix : '') + item;
                        });
                    }
                    return retArr;
                }

                return sortPrefix + order;
            };

            vm.enrichmentsFilter = function () {
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

                if (vm.section == 'wizard.ratingsengine_segment') {
                    filter.IsRatingsEngineAttribute = (vm.metadata.toggle.show.selected_ratingsengine_attributes ? true : '');
                }

                if (DataCloudStore.ratingIterationFilter == 'used') {
                    filter.ImportanceOrdering = '!!';
                }

                if (DataCloudStore.ratingIterationFilter == 'warnings') {
                    filter.HasWarnings = '!!';
                }

                if (DataCloudStore.ratingIterationFilter == 'disabled') {                    
                    filter.ApprovedUsage = 'None';
                }

                return filter;
            };

            vm.subcategoryFilter = function (subcategory) {
                if (!vm.enrichments_completed) {
                    return true;
                }
                var category = vm.category,
                    count = vm.subcategoryCount(category, subcategory);

                return (count ? true : false);
            };

            vm.goBackToModelRules = function () {
                SegmentStore.sanitizeRuleBuckets(RatingsEngineStore.getRule().rule, true)
                $state.go('home.ratingsengine.dashboard.segment.attributes.rules');
            };

            vm.showAtributeAdmin = function () {
                if (vm.section == 'insight' || vm.section == 'wizard.ratingsengine_segment') {
                    return false;
                }

                return ['segment.analysis'].indexOf(vm.section) != -1 && !vm.inWizard;
            };

            vm.showFileImport = function () {
                var flags = FeatureFlagService.Flags();
                var featureFlags = {};
                featureFlags[flags.VDB_MIGRATION] = false;
                featureFlags[flags.ENABLE_FILE_IMPORT] = true;

                return ['segment.analysis'].indexOf(vm.section) != -1 && !vm.inWizard &&
                    AuthorizationUtility.checkAccessLevel(AuthorizationUtility.excludeExternalUser) && AuthorizationUtility.checkFeatureFlags(featureFlags);

            }
            
            vm.getImportState = () => {
                var flags = FeatureFlagService.Flags();
                if(FeatureFlagService.FlagIsEnabled(flags.ENABLE_MULTI_TEMPLATE_IMPORT)){
                    return 'home.multipletemplates';
                }else{
                    return 'home.importtemplates';
                }
            }

            vm.init_filters();
        }
    };
};
    