// grid view multple of 12 (24), dynamic across
angular.module('common.datacloud.explorer', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('DataCloudController', function(
    $scope, $filter, $timeout, $interval, $window, $document, $q, $state, $stateParams,
    ApiHost, BrowserStorageUtility, ResourceUtility, FeatureFlagService, DataCloudStore, DataCloudService, EnrichmentCount,
    EnrichmentTopAttributes, EnrichmentPremiumSelectMaximum, EnrichmentAccountLookup, LookupStore
){
    var vm = this,
        enrichment_chunk_size = 5000,
        flags = FeatureFlagService.Flags();

    angular.extend(vm, {
        debug: (window.location.search.indexOf('debug=1') > 0),
        label: {
            total: 'Total',
            premium: 'Premium',
            button_download: 'Download All',
            button_download_selected: 'Download Selected',
            button_save: 'Save Changes',
            button_select: 'Enrichment Disabled',
            button_selected: 'Enrichment Enabled',
            button_deselect: 'Enrichment Enabled',
            deselected_messsage: 'Attribute will be turned off for enrichment',
            categories_see_all: 'See All Categories',
            categories_select_all: 'All Categories',
            premiumTotalSelectError: 'Premium attribute limit reached',
            generalTotalSelectError: 'Attribute limit reached',
            no_results: 'No attributes were found',
            saved_alert: 'Your changes have been saved.',
            saving_alert: 'Your changes are being saved. <i class="fa fa-cog fa-spin fa-fw"></i>',
            changed_alert: 'No changes will be saved until you press the \'Save\' button.',
            disabled_alert: 'You have disabled an attribute.'
        },
        orders: {
            attributeLookupMode: [ '-Value', 'DisplayName' ],
            attribute: 'DisplayName',
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
        queryText: '',
        category: '',
        lookupMode: EnrichmentAccountLookup !== null,
        lookupFiltered: EnrichmentAccountLookup,
        LookupResponse: LookupStore.response,
        hasCompanyInfo: (LookupStore.response && LookupStore.response.companyInfo ? Object.keys(LookupStore.response.companyInfo).length : 0),
        count: (EnrichmentAccountLookup ? Object.keys(EnrichmentAccountLookup).length : EnrichmentCount.data),
        show_internal_filter: FeatureFlagService.FlagIsEnabled(flags.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES) && $stateParams.section != 'insights' && $stateParams.section != 'team',
        show_lattice_insights: FeatureFlagService.FlagIsEnabled(flags.LATTICE_INSIGHTS),
        enabledManualSave: false,
        enrichments_loaded: false,
        enrichments_completed: false,
        tmpEnrichmentObj: {},
        enrichmentsObj: {}, // by Category
        enrichmentsMap: {}, // by FieldName, value is enrichments[] index
        enrichments: [],
        subcategoriesList: [],
        status_alert: {},
        _subcategories: [],
        subcategories: [],
        categories: [],
        topAttributes: [],
        selected_categories: {},
        categoryOption: null,
        metadata: DataCloudStore.metadata,
        authToken: BrowserStorageUtility.getTokenDocument(),
        userSelectedCount: 0,
        selectDisabled: 1,
        saveDisabled: 1,
        selectedCount: 0,
        initialized: false,
        enable_category_dropdown: false,
        enable_grid: true,
        section: $stateParams.section,
        category: $stateParams.category,
        subcategory: $stateParams.subcategory,
        categoryCounts: {},
        highlightMetadata: {
            categories: {},
            subcategories: {}
        },
        pagesize: 24
    });

    DataCloudStore.setMetadata('lookupMode', vm.lookupMode);

    vm.download_button.items = [{
            href: '/files/latticeinsights/insights/downloadcsv?onlySelectedAttributes=false&Authorization=' + vm.authToken,
            label: vm.label.button_download,
            icon: 'fa fa-file-o'
        },{
            href: '/files/latticeinsights/insights/downloadcsv?onlySelectedAttributes=true&Authorization=' + vm.authToken,
            label: vm.label.button_download_selected,
            icon: 'fa fa-file-o'
        }
    ];

    /* some rules that might hide the page */
    vm.hidePage = function() {
        if (vm.lookupMode && Object.keys(vm.lookupFiltered).length < 1) {
            return true;
        }

        if (vm.section == 'insights' || vm.section == 'team') {
            if (vm.show_lattice_insights || vm.section == 'insights') {
                return false;
            }

            return true;

        }

        return false;
    }

    vm.setCategory = function(category) {
        vm.category = category;
        DataCloudStore.setMetadata('category', category);
    }

    vm.setSubcategory = function(subcategory) {
        vm.subcategory = subcategory;
        DataCloudStore.setMetadata('subcategory', subcategory);
    }


    vm.updateStateParams = function() {
        $state.go('.', {
            category: vm.category,
            subcategory: vm.subcategory
        }, {
            notify: false
        });
    }

    var clearFilters = function() {
        for(var i in vm.metadata.toggle) {
            for(var j in vm.metadata.toggle[i]) {
                vm.metadata.toggle[i][j] = '';
            }
        }
    }

    vm.init = function() {
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
                if(!vm.category && newvalue) {
                    vm.setCategory(vm.categories[0]);
                    vm.updateStateParams();
                }

                vm.query = vm.queryText;
                vm.queryInProgress = false;

                if (vm.section != 'browse') {
                    vm.updateStateParams();
                }

                var categories = Object.keys(vm.categoryCounts).filter(function(value, index) {
                    return vm.categoryCounts[value] > 0;
                });

                if (categories.length == 1) {
                    vm.setCategory(categories[0]);
                }

                vm.filterEmptySubcategories();
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

        vm.closeHighlighterButtons = function(index){
            var index = index || '';
            for(var i in vm.openHighlighter) {
                if(i !== index && vm.openHighlighter[i].open === true) {
                    vm.openHighlighter[i].open = false;
                }
            }
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

        if (vm.lookupMode && vm.LookupResponse.errorCode) {
            $state.go('home.datacloud.lookup.form');
        }

        getEnrichmentCategories();
        getEnrichmentData();

        DataCloudStore.setMetadata('premiumSelectLimit', (EnrichmentPremiumSelectMaximum.data && EnrichmentPremiumSelectMaximum.data['HGData_Pivoted_Source']) || 10);
        DataCloudStore.setMetadata('generalSelectLimit', 100);
        vm.premiumSelectLimit = DataCloudStore.getMetadata('premiumSelectLimit'); //(EnrichmentPremiumSelectMaximum.data && EnrichmentPremiumSelectMaximum.data['HGData_Pivoted_Source']) || 10;
        vm.generalSelectLimit = DataCloudStore.getMetadata('generalSelectLimit');

        vm.statusMessageBox = angular.element('.status-alert');

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

        if (vm.lookupMode && Object.keys(vm.lookupFiltered).length < 1) {
            vm.status_alert.show = true;
            vm.status_alert.type = 'no_results';
            vm.status_alert.message = 'No results to show';
        }
    }

    vm.sortOrder = function() {
        var sortPrefix = vm.sortPrefix.replace('+','');

        if (!vm.category) {
            return sortPrefix + vm.orders.category;
        } else if (vm.subcategories[vm.category] && vm.subcategories[vm.category].length && !vm.subcategory) {
            return sortPrefix + vm.orders.subcategory;
        } else {
            if (vm.lookupMode && vm.category == 'Technology Profile' || vm.category == 'Website Profile') {
                var sortArr = vm.orders.attributeLookupMode,
                    retArr = [];

                sortArr.forEach(function(item, index) {
                    retArr[index] = (item == 'DisplayName' ? sortPrefix : '') + item;
                });

                return retArr;
            } else {
                return sortPrefix + vm.orders.attribute;
            }
        }
    }

    function walkObject(obj, j) {
        if (obj && j) {
            return obj[j];
        }
    }

    vm.filter = function(items, property, value, debug) {
        var propsList = property.split('.');
        if(debug) {
            console.log('poperty:', property, 'value:', value, 'items:', items);
        }

        for (var i=0, result=[]; i < items.length; i++) {
            var item = propsList.reduce(walkObject, items[i]);

            if (typeof item != 'undefined' && item == value) {
                result.push(items[i]);
            }
        }
        return result;
    }

    var stopNumbersInterval = function(){
        if (numbersInterval) {
            $interval.cancel(numbersInterval);
            vm.placeholderTotal = null;
        }
    }

    var getEnrichmentData = function(opts) {
        var deferred = $q.defer(),
            opts = opts || {};

        opts.max = (EnrichmentCount.data ? enrichment_chunk_size : 100);

        var concurrent = 6, // most browsers allow a max of 6 concurrent connections per domain
            max = Math.ceil(EnrichmentCount.data / concurrent),
            offset = opts.offset || 0,
            iterations = Math.ceil(vm.count / max),
            _store;

        vm.concurrent = concurrent;
        vm.concurrentIndex = 0;

        if (DataCloudStore.enrichments) {
            vm.xhrResult(DataCloudStore.enrichments, true);
        } else {
            for (var j=0; j<iterations; j++) {
                DataCloudStore.getEnrichments({ max: max, offset: j * max }).then(vm.xhrResult);
            }
        }
    }

    vm.cube = [];
    vm.xhrResult = function(result, cached) {
        var _store, key, item;

        if (cached) {
            vm.enrichmentsObj = {};
            DataCloudStore.init();
        }

        vm.concurrentIndex++;

        if (result != null && result.status === 200) {
            if (vm.lookupFiltered !== null) {
                for (var i=0, data=[]; i<result.data.length; i++) {
                    if (vm.lookupFiltered[result.data[i].FieldName]) {
                        data.push(result.data[i]);
                    }
                }
            } else {
                var data = result.data;
            }

            vm.enrichments_loaded = true;
            vm.enrichmentsStored = vm.enrichments.concat(result.data);

            // Updates the Acct Lookup Attributes tab count
            if (vm.lookupMode) {
                LookupStore.add('count', vm.enrichments.length);
            }

            for (key in data) {
                item = data[key];

                if (!vm.enrichmentsObj[item.Category]) {
                    vm.enrichmentsObj[item.Category] = [];
                }

                vm.enrichmentsMap[item.FieldName] = vm.enrichments.length;
                vm.enrichmentsObj[item.Category].push(item);
                vm.enrichments.push(item);
            }

            numbersNumber = 0;
            _store = result; // just a copy of the correct data structure and properties for later

            if (cached || vm.enrichments.length >= vm.count || vm.concurrentIndex >= vm.concurrent) {
                _store.data = vm.enrichmentsStored; // so object looks like what a typical set/get in the store wants with status, config, etc
                DataCloudStore.setEnrichments(_store); // we do the store here because we only want to store it when we finish loading all the attributes
                vm.hasSaved = vm.filter(vm.enrichments, 'IsDirty', true).length;
                vm.enrichments_completed = true;
                vm.generateTree(true);
            } else {
                vm.generateTree();
            }

            if(vm.enrichments_completed) {
                getEnrichmentCube().then(function(result){
                    vm.cube = result.data;
                });
            }
        }

        var selectedTotal = vm.filter(vm.enrichments, 'IsSelected', true);
            DisabledForSalesTeamTotal = vm.filter(vm.enrichments, 'AttributeFlagsMap.CompanyProfile.hidden', true),
            EnabledForSalesTeamTotal = vm.filter(vm.enrichments, 'IsInternal', false).length - DisabledForSalesTeamTotal.length;

        if(!vm.lookupMode) {
            DataCloudStore.setMetadata('generalSelectedTotal', selectedTotal.length);
            DataCloudStore.setMetadata('premiumSelectedTotal', vm.filter(selectedTotal, 'IsPremium', true).length);
            DataCloudStore.setMetadata('enrichmentsTotal', vm.enrichments.length);
            DataCloudStore.setMetadata('enabledForSalesTeamTotal', EnabledForSalesTeamTotal);
        }

        vm.generalSelectedTotal = DataCloudStore.getMetadata('generalSelectedTotal');
        vm.premiumSelectedTotal = DataCloudStore.getMetadata('premiumSelectedTotal');
    }

    vm.generateTree = function(isComplete) {
        var obj = vm.tmpEnrichmentObj;

        vm.enrichments.forEach(function(item, index) {
            var category = item.Category;
            var subcategory = item.Subcategory;

            if (!obj[category]) {
                obj[category] = {};
            }

            if (!obj[category][subcategory]) {
                obj[category][subcategory] = [];
            }

            if (vm.lookupMode && vm.lookupFiltered[item.FieldName]) {
                item.AttributeValue = vm.lookupFiltered[item.FieldName];
            }

            item.HighlightState = highlightOptionsInitState(item);
            item.HighlightHidden = (item.AttributeFlagsMap && item.AttributeFlagsMap.CompanyProfile && item.AttributeFlagsMap.CompanyProfile.hidden === true ? true : false);
            item.HighlightHighlighted = (item.AttributeFlagsMap && item.AttributeFlagsMap.CompanyProfile && item.AttributeFlagsMap.CompanyProfile.highlighted ? item.AttributeFlagsMap.CompanyProfile.highlighted : false);

            var highlightMetadataFlags = {
                    allDisabled: false
                };
            if(!item.AttributeFlagsMap || !item.AttributeFlagsMap.CompanyProfile) {
                item.AttributeFlagsMap = {};
                item.AttributeFlagsMap.CompanyProfile = {};
            } else if(item.AttributeFlagsMap && item.AttributeFlagsMap.CompanyProfile && Object.keys(item.AttributeFlagsMap.CompanyProfile).length) {
                // need to know if all are disabled as well
                highlightMetadataFlags.hasDirty = true;

                if(item.AttributeFlagsMap.CompanyProfile.hidden === false) {
                    highlightMetadataFlags.hasEnabled = true;
                }
                if(item.AttributeFlagsMap.CompanyProfile.hidden === true) {
                    highlightMetadataFlags.hasDisabled = true;
                    if(vm.highlightMetadata.categories[item.Category]) {
                        if(vm.highlightMetadata.categories[item.Category].hasEnabled) {
                            vm.highlightMetadata.categories[item.Category].allDisabled = false;
                        } else {
                            vm.highlightMetadata.categories[item.Category].allDisabled = true;
                        }
                    }
                 }
                vm.highlightMetadata.categories[item.Category] = highlightMetadataFlags;
                vm.highlightMetadata.subcategories[item.Subcategory] = highlightMetadataFlags;
            }

            obj[category][subcategory].push(index);
        });

        if (isComplete) {
            vm.categories.forEach(function(category, item) {
                if (obj[category]) {
                    getEnrichmentSubcategories(category, Object.keys(obj[category]));
                }
            });

            getTopAttributes();
        }
        //console.log(vm.highlightMetadata); ben
    }

    vm.highlightTypes = {
        enabled: 'Enabled for Sales Team',
        disabled: 'Disabled for Sales Team',
        highlighted: 'Highlight for Sales Team'
    }

    vm.highlightTypesCategory = {
        enabled: vm.highlightTypes.enabled,
        disabled: vm.highlightTypes.disabled,
        dirty: 'some enabled for sales team'
    }

    var highlightOptionsInitState = function(enrichment) {
        var ret = {type: 'enabled', label: '', highlighted: false, enabled: false};

        if(!enrichment.AttributeFlagsMap || !enrichment.AttributeFlagsMap.CompanyProfile) {
            ret.type = 'enabled';
            ret.label = vm.highlightTypes[ret.type];
            ret.enabled = true;
            return ret;
        }

        ret.enabled = !enrichment.AttributeFlagsMap.CompanyProfile.hidden;
        ret.dirty = false;

        if(enrichment.AttributeFlagsMap.CompanyProfile.hidden === true) {
            ret.type = 'disabled';
            ret.enabled = false;
            ret.dirty = true;
        }
        if(enrichment.AttributeFlagsMap.CompanyProfile.hidden === false) {
            ret.type = 'enabled';
            ret.enabled = true;
            ret.dirty = true;
        }
        if(enrichment.AttributeFlagsMap.CompanyProfile.highlighted === true) {
            ret.type = 'highlighted';
            ret.highlighted = true;
            ret.enabled = true;
            ret.dirty = true;
        }
        if(ret.type) {
            ret.label = vm.highlightTypes[ret.type];
        }

        return ret;
    }

    vm.getArray = function(number) {
        return new Array(number);
    }

    var getFlags = function(opts) {
        var deferred = $q.defer();

        DataCloudStore.getFlags(opts).then(function(result) {
            deferred.resolve(result);
        });
        return deferred.promise;
    }

    var setFlags = function(opts, flags) {
        var deferred = $q.defer();
        DataCloudService.setFlags(opts, flags).then(function(result) {
            deferred.resolve(result);
        });
        return deferred.promise;
    }

    vm.setFlags = function(type, enrichment) {
        if(type === enrichment.HighlightState.type) {
            return false;
        }
        var flags = {
                "hidden": true,
                "highlighted": false
            },
            label = vm.highlightTypes[type] || 'unknown type',
            enabled = false,
            opts = {
                fieldName: enrichment.FieldName
            };

        if(type === 'highlighted') {
            flags.hidden = false;
            flags.highlighted = true;
        } else if(type === 'enabled') {
            flags.hidden = false;
        } else if(type === 'disabled') {
            flags.hidden = true;
            flags.highlighted = false;
        }

        vm.statusMessage(vm.label.saving_alert, {wait: 0});

        setFlags(opts, flags).then(function(){
            vm.statusMessage(vm.label.saved_alert, {type: 'saved'});

            enrichment.HighlightState = {type: type, label: label, enabled: !flags.hidden, highlighted: flags.highlighted};
            enrichment.HighlightHidden = flags.hidden;
            enrichment.HighlightHighlighted = flags.highlighted;

            vm.enrichments.find(function(i){return i.FieldName === enrichment.FieldName;}).AttributeFlagsMap.CompanyProfile = flags;
            DataCloudStore.updateEnrichments(vm.enrichments);

            var DisabledForSalesTeamTotal = vm.filter(vm.enrichments, 'HighlightHidden', true),
                EnabledForSalesTeamTotal = vm.filter(vm.enrichments, 'IsInternal', false).length - DisabledForSalesTeamTotal.length;

            DataCloudStore.setMetadata('enabledForSalesTeamTotal', EnabledForSalesTeamTotal);

        });
    }

    var setFlagsByCategory = function(opts, flags) {
        var deferred = $q.defer();
        if(opts.subcategoryName) {
            DataCloudService.setFlagsBySubcategory(opts, flags).then(function(result) {
                deferred.resolve(result);
            });
        } else if(opts.categoryName) {
            DataCloudService.setFlagsByCategory(opts, flags).then(function(result) {
                deferred.resolve(result);
            });
        }
        return deferred.promise;
    }

    vm.setFlagsByCategory = function(type, category, subcategory){
        var opts = {
            categoryName: category,
            subcategoryName: subcategory
        },
        flags = {
            hidden: (type === 'enabled' ? false : true),
        };

        vm.statusMessage(vm.label.saving_alert, {wait: 0});

        setFlagsByCategory(opts, flags).then(function(){
            vm.statusMessage(vm.label.saved_alert, {type: 'saved'});
            var enrichments = vm.filter(vm.enrichments, 'Category', category);
            if(subcategory) {
                enrichments = vm.filter(enrichments, 'Subcategory', subcategory);
            }
            for(var i in enrichments) {
                var enrichment = enrichments[i],
                flags = {},
                label = (enrichment.HighlightHighlighted ? enrichment.HighlightState.label : vm.highlightTypes[type]),
                wasHighlighted = enrichment.HighlightHighlighted,
                _type = type;

                if(type === 'disabled') {
                    label = vm.highlightTypes[type];
                    flags.highlighted = false;
                    flags.hidden = true;
                    enrichment.HighlightHighlighted = false;
                    enrichment.HighlightState.highlighted = false;
                }

                if(type === 'enabled') {
                    flags.hidden = false;
                    if(wasHighlighted) {
                        _type = 'highlighted';
                    }
                }

                enrichment.AttributeFlagsMap.CompanyProfile.hidden = flags.hidden
                enrichment.HighlightHidden = flags.hidden;
                enrichment.HighlightState.type = _type;
                enrichment.HighlightState.label = label;
                enrichment.HighlightState.enabled = !flags.hidden;

                flags = {};
            }
            DataCloudStore.updateEnrichments(vm.enrichments);

            var DisabledForSalesTeamTotal = vm.filter(vm.enrichments, 'HighlightHidden', true),
                EnabledForSalesTeamTotal = vm.filter(vm.enrichments, 'IsInternal', false).length - DisabledForSalesTeamTotal.length;

            DataCloudStore.setMetadata('enabledForSalesTeamTotal', EnabledForSalesTeamTotal);
        });
    }

    vm.filterLookupFiltered = function(item, type) {
        if (item === undefined || item === null) {
            return item;
        }

        switch (type) {
            case 'PERCENTAGE':
                var percentage = Math.round(item * 100);
                return percentage !== NaN ? percentage + '%' : '';
            case 'NUMERIC':
                return $filter('number')(parseInt(item, 10));
            case 'CURRENCY':
                return ResourceUtility.getString('CURRENCY_SYMBOL') + $filter('number')(parseInt(item, 10));
            case 'DATE':
                var date = new Date(parseInt(item, 10));
                var year = date.getFullYear().toString();
                var month = (date.getMonth() + 1).toString();
                month = month.length === 2 ? month : 0 + month;
                var day = date.getDate().toString();
                day = day.length === 2 ? day : '0' + day;
                return '' + year + month + day;
            case 'URI':
            case 'ALPHA':
            case 'BOOLEAN':
            case 'ENUM':
            case 'EMAIL':
            case 'PHONE':
            case 'YEAR':
            default:
                return item;
        }

        return item;
    }

    var getTopAttributes = function(opts) {
        var opts = opts || {},
            category = opts.category;

        Object.keys(EnrichmentTopAttributes).forEach(function(catKey) {
            var category = data[catKey]['SubCategories'];

            Object.keys(category).forEach(function(subcategory) {
                var items = category[subcategory];

                items.forEach(function(item) {
                    var enrichment = vm.enrichments[vm.enrichmentsMap[item.Attribute]];

                    if (enrichment && enrichment.DisplayName) {
                        var displayName = enrichment.DisplayName;

                        item.DisplayName = displayName;
                    }
                });
            });
        });

        vm.topAttributes = EnrichmentTopAttributes;
    }

    var getEnrichmentCategories = function() {
        if (EnrichmentTopAttributes) {
            vm.categories = Object.keys(EnrichmentTopAttributes).sort();
            for(var i in vm.categories) {
                vm.categoryCounts[vm.categories[i]] = null;
            }
            vm.enable_category_dropdown = true;
        }
    }

    var subcategoriesExclude = [];
    var getEnrichmentSubcategories = function(category, subcategories) {
        vm._subcategories[category] = subcategories;
        vm.subcategories[category] = subcategories;

        if (subcategories.length <= 1) {
            subcategoriesExclude.push(category);
        }
        vm.filterEmptySubcategories();
    }

    vm.getTileTableItems = function(category, subcategory, segment, limit, debug) {
        var items = [],
            limit = (limit === 0 ? 0 : null) || limit || null;

        if (vm.topAttributes[category]) {
            if (!subcategory && vm.isYesNoCategory(category, true)) {
                Object.keys(vm.topAttributes[category].SubCategories).forEach(function(key, index) {
                    items = items.concat(vm.topAttributes[category].SubCategories[key]);
                })
            } else {
                items = vm.topAttributes[category].SubCategories[subcategory || 'Other'];
            }

            if (!vm.lookupMode) {
                items.forEach(function(item) {
                    var index = vm.enrichmentsMap[item.Attribute];
                    var enrichment = vm.enrichments[index];
                    Object.assign(item, enrichment);
                });
            }
        }

        if (vm.lookupMode || (!items || items.length == 0)) {
            items = vm.enrichmentsObj[category];

            if (subcategory || vm.isYesNoCategory(category)) {
                var subcategory = subcategory || 'Other';

                items = items.filter(function(item) {
                    var isSubcategory = item.Subcategory == subcategory;
                    var attrValue = vm.lookupFiltered[item.FieldName];

                    if (vm.lookupMode && attrValue && isSubcategory) {
                        item.Value = attrValue;
                        if ((!vm.metadata.toggle.show.nulls && attrValue == 'No')) {
                            return false;
                        } else {
                            return true;
                        }
                    }

                    return isSubcategory;
                });
            }
        }
        if(segment && items) { //ben
            var segmented = vm.filter(items, segment, true),
                other = vm.filter(items, segment, false);

            segmented.length = Object.keys(segmented).length;
            other.length = Object.keys(other).length;

            var remainder = (limit - segmented.length);

            _items = {};
            _items[segment] = segmented;

            if(remainder > 0) {
                _items['other'] = Array.prototype.slice.call(other, 0, remainder);
            }
            items = _items;
        }
        return items;
    }

    vm.generateTileTableLabel = function(items) {
        return items
            ? 'Top ' + (items.length > 1 ? items.length + ' attributes' : 'attribute')
            : '';
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

    vm.inCategory = function(enrichment){
        if (enrichment.DisplayName && !(_.size(vm.selected_categories))) { // for case where this is used as a | filter in the enrichments ngRepeat on initial state
            return true;
        }

        var selected = (typeof vm.selected_categories[enrichment.Category] === 'object');

        return selected;
    };

    vm.inSubcategory = function(enrichment){
        var category = vm.selected_categories[enrichment.Category],
            subcategories = (category && category['subcategories'] ? category['subcategories'] : []),
            subcategory = enrichment.Subcategory;

        if (enrichment.DisplayName && !subcategories.length) { // for case where this is used as a | filter in the enrichments ngRepeat on initial state
            return true;
        }

        if (!subcategories.length) {
            return false;
        }

        var selected = (typeof category === 'object' && subcategories.indexOf(subcategory) > -1);
        return selected;
    };

    vm.categoryClass = function(category){
        var category = 'category-' + category.toLowerCase().replace(' ','-');
        return category;
    }

    vm.selectEnrichment = function(enrichment){
        vm.saveDisabled = 0;
        vm.selectDisabled = 0;
        var selectedTotal = vm.filter(vm.enrichments, 'IsSelected', true);

        if(enrichment.IsPremium) {
            vm.premiumSelectedTotal = vm.filter(selectedTotal, 'IsPremium', true).length;
            if(vm.premiumSelectedTotal > vm.premiumSelectLimit) {
                vm.premiumSelectedTotal = vm.premiumSelectLimit;
                enrichment.IsSelected = false;
                enrichment.IsDirty = false;
                vm.statusMessage(vm.label.premiumTotalSelectError);
                return false;
            }
        }

        vm.generalSelectedTotal = selectedTotal.length;

        if (vm.generalSelectedTotal > vm.generalSelectLimit) {
            vm.generalSelectedTotal = vm.generalSelectLimit;
            enrichment.IsSelected = false;
            enrichment.IsDirty = false;
            vm.statusMessage(vm.label.generalTotalSelectError);
            return false;
        }

        if (enrichment.IsSelected){
            vm.userSelectedCount++;
            vm.statusMessage(vm.label.changed_alert);
        } else {
            vm.userSelectedCount--;
            if(!enrichment.WasDirty) {
                enrichment.WasDirty = true;
                var notselected = vm.filter(vm.enrichments, 'IsSelected', false).length;
                vm.disabled_count = vm.filter(notselected, 'IsDirty', true).length;
                vm.label.disabled_alert = '<p><strong>You have disabled ' + vm.disabled_count + ' attribute' + (vm.disabled_count > 1 ? 's' : '') + '</strong>. If you are using any of these attributes for real-time scoring, these attributes will no longer be updated in your system.</p>';
                vm.label.disabled_alert += '<p>No changes will be saved until you press the \'Save\' button.</p>';
                vm.statusMessage(vm.label.disabled_alert, {type: 'disabling', wait: 0});
            }
        }

        if (vm.userSelectedCount < 1) {
            vm.selectDisabled = 1;
        }

        if (!vm.enabledManualSave) {
            vm.saveSelected();
        }
    }

    var status_timer;
    vm.statusMessage = function(message, opts, callback) {
        var opts = opts || {},
            wait = (opts.wait || opts.wait === 0 ? opts.wait : 3000),
            type = opts.type || 'alert';

        vm.status_alert.type = type;
        vm.status_alert.message = message;
        $timeout.cancel(status_timer);
        vm.status_alert.show = true;

        if(wait) {
            status_timer = $timeout(function(){
                vm.status_alert.show = false;
                vm.status_alert.message = '';
                if(typeof callback === 'function') {
                    callback();
                }
            }, wait);
        }
    }

    vm.closeStatusMessage = function() {
        $timeout.cancel(status_timer);
        vm.status_alert.show = false;
        vm.status_alert.message = '';
    }

    vm.saveSelected = function(){
        var dirtyEnrichments = vm.filter(vm.enrichments, 'IsDirty', true),
            selectedObj = vm.filter(dirtyEnrichments, 'IsSelected', true),
            deselectedObj = vm.filter(dirtyEnrichments, 'IsSelected', false),
            selected = [],
            deselected = [];

        vm.selectDisabled = (selectedObj.length ? 0 : 1);

        for(i in selectedObj) {
            selected.push(selectedObj[i].FieldName);
        }

        for(i in deselectedObj) {
            deselected.push(deselectedObj[i].FieldName);
        }

        var data = {
            selectedAttributes: selected,
            deselectedAttributes: deselected
        }

        vm.saveDisabled = 1;
        vm.hasSaved = 0;

        vm.statusMessage(vm.label.saving_alert, {wait: 0});

        DataCloudService.setEnrichments(data).then(function(result){
            vm.statusMessage(vm.label.saved_alert, {type: 'saved'});
            vm.saveDisabled = 1;
            if(selectedObj.length > 0 || deselectedObj.length > 0) {
                var dirtyObj = vm.filter(vm.enrichments, 'IsDirty', true);
                for(i in dirtyObj){
                    dirtyObj[i].IsDirty = false;
                }
            }
        });
    }

    vm.fieldType = function(fieldType){
        var fieldType = fieldType.replace(/[0-9]+/g, '*');
        var fieldTypes = {
            'default':'Text/String',
            'NVARCHAR(*)':'Text/String',
            'FLOAT':'Number/Float',
            'INT':'Number/Int',
            'BOOLEAN':'Boolean'
        }
        return fieldTypes[fieldType] || fieldTypes.default;
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

        if(vm.section == 'team' || vm.section == 'insights') {
            filter.HighlightHidden = (!vm.metadata.toggle.hide.enabled ? '' : true) || (!vm.metadata.toggle.show.enabled ? '' : false);
            filter.HighlightHighlighted = (!vm.metadata.toggle.show.highlighted ? '' : true) || (!vm.metadata.toggle.hide.highlighted ? '' : false);
        }

        if (vm.lookupMode && vm.isYesNoCategory(vm.category)) {
            filter.AttributeValue = (!vm.metadata.toggle.show.nulls ? '!' + 'No' : '');
        }

        return filter;
    }

    var subcategoryRenamer = function(string, replacement){
        if (string) {
            var replacement = replacement || '';
            return string.toLowerCase().replace(/\W+/g, replacement);
        }
        return '';
    }

    vm.subcategoryIcon = function(category, subcategory){
        var path = '/assets/images/enrichments/subcategories/',
            category = subcategoryRenamer(category),
            subcategory = subcategoryRenamer(subcategory),
            icon = category + (subcategory ? '-'+subcategory : '') + '.png';

        return path + icon;
    }

    vm.subcategoryFilter = function(subcategory) {
        if(!vm.enrichments_completed) {
            return true;
        }
        var category = vm.category,
            count = vm.subcategoryCount(category, subcategory);

        return (count ? true : false);
    }

    vm.subcategoryCount = function(category, subcategory) {
        var filtered = vm.enrichmentsObj[category];

        if (!filtered || filtered.length <= 0) {
            return 0;
        }

        for (var i=0, result=[]; i < filtered.length; i++) {
            var item = filtered[i];
            if (item && vm.searchFields(item)) {
                if ((item.Category != category)
                || (item.Subcategory != subcategory)
                || (vm.lookupMode && !vm.metadata.toggle.show.nulls && item.AttributeValue == "No" && vm.isYesNoCategory(category))
                || (vm.metadata.toggle.show.selected && !item.IsSelected)
                || (vm.metadata.toggle.hide.selected && item.IsSelected)
                || (vm.metadata.toggle.show.premium && !item.IsPremium)
                || (vm.metadata.toggle.hide.premium && item.IsPremium)
                || (!vm.metadata.toggle.show.internal && item.IsInternal)
                || (vm.metadata.toggle.show.enabled && item.HighlightHidden)
                || (vm.metadata.toggle.hide.enabled && !item.HighlightHidden)
                || (vm.metadata.toggle.show.highlighted && !item.HighlightHighlighted)
                || (vm.metadata.toggle.hide.highlighted && item.HighlightHighlighted)) {
                    continue;
                }
                result.push(item);
            }
        }

        return result.length;
    }

    vm.subcategoryClick = function(subcategory, $event) {
        var target = angular.element($event.target),
            currentTarget = angular.element($event.currentTarget);

        if(target.closest("[ng-click]")[0] !== currentTarget[0]) {
            // do nothing, user is clicking something with it's own click event
        } else {
            vm.setSubcategory((vm.subcategory === subcategory ? '' : subcategory));
            vm.metadata.current = 1;
            vm.updateStateParams();
        }
    }

    vm.categoryCount = function(category) {
        var filtered = vm.enrichmentsObj[category];

        if (!filtered) {
            return 0;
        }

        for (var i=0, result=[]; i < filtered.length; i++) {
            var item = filtered[i];
            if (item && vm.searchFields(item)) {
                if ((item.Category != category)
                || (vm.lookupMode && !vm.metadata.toggle.show.nulls && item.AttributeValue == "No" && vm.isYesNoCategory(category))
                || (vm.metadata.toggle.show.selected && !item.IsSelected)
                || (vm.metadata.toggle.hide.selected && item.IsSelected)
                || (vm.metadata.toggle.show.premium && !item.IsPremium)
                || (vm.metadata.toggle.hide.premium && item.IsPremium)
                || (!vm.metadata.toggle.show.internal && item.IsInternal)
                || (vm.metadata.toggle.show.enabled && item.HighlightHidden)
                || (vm.metadata.toggle.hide.enabled && !item.HighlightHidden)
                || (vm.metadata.toggle.show.highlighted && !item.HighlightHighlighted)
                || (vm.metadata.toggle.hide.highlighted && item.HighlightHighlighted)) {
                    continue;
                }
                result.push(item);
            }
        }
        vm.categoryCounts[item.Category] = result.length;
        if(result.length <= 1) {
            //gotoNonemptyCategory();
        }
        return result.length;
    }

    /* jumps you to non-empty category when you filter */
    var gotoNonemptyCategory = function() {
        var categories = [],
            category = '';
        if(vm.category) {
            for(var i in vm.categoryCounts) {
                if(vm.categoryCounts[i] > 0) {
                    categories.push(i);
                }
            }
        }
        if(categories.length <= 1) {
            vm.setCategory(categories[0]);
        }
    }

    vm.categoryIcon = function(category){
        var path = '/assets/images/enrichments/subcategories/',
            category = subcategoryRenamer(category, ''),
            icon = category + '.png';

        return path + icon;
    }

    vm.categoryClick = function(category, $event) {
        var target = angular.element($event.target),
            currentTarget = angular.element($event.currentTarget);

        if(target.closest("[ng-click]")[0] !== currentTarget[0]) {
            // do nothing, user is clicking something with it's own click event
        } else {
            var category = category || '';
            if(vm.subcategory && vm.category == category) {
                vm.setSubcategory('');
                if(subcategoriesExclude.indexOf(category) >= 0) { // don't show subcategories
                    vm.setSubcategory(vm.subcategories[category][0]);
                }
            } else if(vm.category == category) {
                vm.setSubcategory('');
                //vm.category = '';
            } else {
                vm.setSubcategory('');
                if(subcategoriesExclude.indexOf(category)) {
                    vm.setSubcategory(vm.subcategories[category][0]);
                }
                vm.setCategory(category);

                vm.filterEmptySubcategories();
            }
            vm.metadata.current = 1;
            vm.updateStateParams();
        }
    }

    vm.companyInfoFormatted = function (type, value) {
        if (!vm.LookupResponse.companyInfo) {
            return false;
        }
        if(!vm.LookupResponse || !vm.LookupResponse.companyInfo){
            return false;
        }
        var value = value || '',
            info = vm.LookupResponse.companyInfo;

        switch (type) {
            case 'address':
                var address = [];
                if(info.LDC_Street) {
                    address.push(info.LDC_Street);
                }
                if(info.LDC_City) {
                    address.push(info.LDC_City);
                }
                if(info.LDC_State) {
                    address.push(info.LDC_State);
                }
                if(info.LDC_ZipCode) {
                    address.push(info.LDC_ZipCode.substr(0,5) + ',');
                }
                if(info.LE_COUNTRY) {
                    address.push(info.LE_COUNTRY);
                }
                return address.join(' ');
            break;
            case 'phone':
                if(info.LE_COMPANY_PHONE) {
                    var phone = info.LE_COMPANY_PHONE;
                    return phone.replace(/\D+/g, '').replace(/(\d{3})(\d{3})(\d{4})/, '($1) $2-$3');
                }
            break;
            case 'range':
                if(value) {
                    var range = value;
                    range = range.replace('-',' - ');
                    return range;
                }
            break;
        }
    }

    vm.isYesNoCategory = function(category, includeKeywords) {
        var list = ['Website Profile','Technology Profile'];

        if (includeKeywords) {
            list.push('Website Keywords');
        }

        return list.indexOf(category) >= 0;
    }

    var addUniqueToArray = function(array, item) {
        if (array && item && !array.indexOf(item)) {
            array.push(item);
        }
    }

    var removeFromArray = function(array, item) {
        if (array && item) {
            var index = array.indexOf(item);
            if (index > -1) {
                array.splice(index, 1);
            }
        }
    }

    vm.filterEmptySubcategories = function() {
        if (vm._subcategories[vm.category]) {
            for (var i=0, newCategories = []; i<vm._subcategories[vm.category].length; i++) {
                var subcategory = vm._subcategories[vm.category][i];

                if (vm.subcategoryCount(vm.category, subcategory) > 0) {
                    newCategories.push(subcategory);
                }
            }

            if (newCategories.length <= 1) {

                addUniqueToArray(subcategoriesExclude, vm.category);
                vm.setSubcategory(newCategories[0]);
                vm.updateStateParams();
            } else {

                if (subcategoriesExclude.indexOf(vm.category)) {
                    vm.setSubcategory('');
                    vm.updateStateParams();
                }
                removeFromArray(subcategoriesExclude, vm.category);
            }
            vm.subcategories[vm.category] = newCategories;
        }
    }

    vm.percentage = function(number, total, suffix, limit) {
        var suffix = suffix || '',
            percentage = 0;
        if (number && total) {
            percentage = ((number / total) * 100);
            if(typeof limit != 'undefined') {
                percentage = percentage.toFixed(limit);
            }
            return percentage + suffix;
        }
        return 0;
    }

    var getEnrichmentCube = function() {
        var deferred = $q.defer();

        DataCloudStore.getCube().then(function(result) {
            deferred.resolve(result);
        });
        return deferred.promise;
    }

    vm.init();
})
.directive('fallbackSrc', function () {
    var fallbackSrc = {
        link: function postLink(scope, iElement, iAttrs) {
            iElement.bind('error', function() {
                angular.element(this).attr("src", iAttrs.fallbackSrc);
            });
        }
    }
    return fallbackSrc;
});
