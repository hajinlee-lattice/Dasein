// grid view multple of 12 (24), dynamic across
angular.module('common.datacloud.explorer', [
    'common.datacloud.explorer.filters',
    'common.datacloud.explorer.companyprofile',
    'common.datacloud.explorer.latticeratingcard',
    'common.datacloud.explorer.categorytile',
    'common.datacloud.explorer.subcategorytile',
    'common.datacloud.explorer.attributetile',
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('DataCloudController', function(
    $scope, $filter, $timeout, $interval, $window, $document, $q, $state, $stateParams,
    ApiHost, BrowserStorageUtility, ResourceUtility, FeatureFlagService, DataCloudStore, DataCloudService,
    EnrichmentTopAttributes, EnrichmentPremiumSelectMaximum, LookupStore, QueryStore,
    SegmentServiceProxy, QueryRestriction, CurrentConfiguration, EnrichmentCount, LookupResponse
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
            button_import_data: 'Import Data',
            button_refine_query: 'Refine Query',
            button_save_segment: 'Save Segment',
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
        EnrichmentPremiumSelectMaximum: EnrichmentPremiumSelectMaximum,
        category: '',
        lookupMode: (LookupResponse && LookupResponse.attributes !== null),
        lookupFiltered: LookupResponse.attributes,
        LookupResponse: LookupStore.response,
        no_lookup_results_message : false,
        hasCompanyInfo: (LookupStore.response && LookupStore.response.companyInfo ? Object.keys(LookupStore.response.companyInfo).length : 0),
        count: (LookupResponse.attributes ? Object.keys(LookupResponse.attributes).length : EnrichmentCount.data),
        show_internal_filter: FeatureFlagService.FlagIsEnabled(flags.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES) && $stateParams.section != 'insights' && $stateParams.section != 'team',
        show_lattice_insights: FeatureFlagService.FlagIsEnabled(flags.LATTICE_INSIGHTS),
        show_segmentation: FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL),
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
        subcategoriesExclude: [],
        categories: [],
        topAttributes: [],
        cube: [],
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
        openHighlighter: {},
        categoryCounts: {},
        TileTableItems: {},
        workingBuckets: CurrentConfiguration,
        highlightMetadata: {
            categories: {}
        },
        pagesize: 24,
        categorySize: 7
    });

    DataCloudStore.setMetadata('lookupMode', vm.lookupMode);
    /* some rules that might hide the page */
    vm.hidePage = function() {
        if (vm.lookupMode && Object.keys(vm.lookupFiltered).length < 1) {
            return true;
        }
        if (vm.section == 'insights' || vm.section == 'team') {
            if (vm.show_lattice_insights) {
                return false;
            }
            return true;
        }
        return false;
    }

    vm.relanding = function(huh) {
        /*
         * Rebuild the tile table items
         */
        vm.TileTableItems = {};
        if(vm.metadataSegments || QueryRestriction) {
            getExplorerSegments(vm.enrichments);
        }
    }

    vm.filter = function(items, property, value, debug) {
        if (property.indexOf('.') > -1) {
            var propsList = property.split('.');
            var walkObject = function(obj, j) {
                if (obj && j) {
                    return obj[j];
                }
            };

            for (var i=0, result=[], item; i < items.length; i++) {
                item = propsList.reduce(walkObject, items[i]);

                if (typeof item != 'undefined' && item == value) {
                    result.push(items[i]);
                }
            }
        } else {
            var result = items.filter(function(item) {
                return item[property] == value;
            });
        }

        if (debug) {
            console.log('prop:', property, 'value:', value, 'items:', items, 'result:', result);
        }

        return result;
    }

    vm.init = function() {
        if (vm.lookupMode && vm.LookupResponse.errorCode) {
            $state.go('home.datacloud.lookup.form');
        }

        getEnrichmentCategories();
        getEnrichmentData();

        vm.statusMessageBox = angular.element('.status-alert');

        if (vm.lookupMode && Object.keys(vm.lookupFiltered).length < 1) {
            //vm.statusMessage('No results to show', {type: 'no_results', wait: 0});
            vm.no_lookup_results_message = true;
        }

        if(vm.section === 'segment.analysis') {
            vm.metadataSegments = QueryRestriction;
        }
    }

    vm.closeHighlighterButtons = function(index) {
        var index = index || '';
        for(var i in vm.openHighlighter) {
            if(vm.openHighlighter[i].open) {
                vm.openHighlighter[i].open = false;
                if(!index) {
                    if(!$scope.$$phase) { // this is bad but short term hack
                        $scope.$digest(); // this works, but at what cost?! -- this also breaks because of the $digest in filters.component.js document click.  this needs much better solution.
                    }
                }
            }
        }
    }

    vm.setSubcategory = function(subcategory) {
        vm.subcategory = subcategory;
        DataCloudStore.setMetadata('subcategory', subcategory);
    }

    vm.subcategoryRenamer = function(string, replacement){
        if (string) {
            var replacement = replacement || '';
            return string.toLowerCase().replace(/\W+/g, replacement);
        }

        return '';
    }

    vm.updateStateParams = function() {
        $state.go('.', {
            category: vm.category,
            subcategory: vm.subcategory
        }, {
            notify: false
        });
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
            iterations = Math.ceil(EnrichmentCount.data / max),
            _store;

        vm.concurrent = concurrent;
        vm.concurrentIndex = 0;

        if (DataCloudStore.enrichments) {
            vm.xhrResult(DataCloudStore.enrichments, true);
        } else {
            if (DemoData) {
                vm.xhrResult({
                    data: DemoData.enrichments,
                    status: 200,
                });
            }

            for (var j=0; j<iterations; j++) {
                DataCloudStore.getEnrichments({ max: max, offset: j * max }).then(vm.xhrResult);
            }
        }
    }

    vm.xhrResult = function(result, cached) {
        var timestamp = new Date().getTime();
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
            var timestamp2 = new Date().getTime();

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

            if (cached || vm.enrichments.length >= EnrichmentCount.data || vm.concurrentIndex >= vm.concurrent) {
                _store.data = vm.enrichmentsStored; // so object looks like what a typical set/get in the store wants with status, config, etc
                DataCloudStore.setEnrichments(_store); // we do the store here because we only want to store it when we finish loading all the attributes
                vm.hasSaved = vm.filter(vm.enrichments, 'IsDirty', true).length;
                vm.enrichments_completed = true;

                var timestamp3 = new Date().getTime();
                vm.generateTree(true);
            } else {
                var timestamp3 = new Date().getTime();
                vm.generateTree();
            }
            var timestamp4 = new Date().getTime();

            if(vm.enrichments_completed) {
                getEnrichmentCube().then(function(result){
                    result.data.Stats = angular.extend({}, result.data.Stats, DemoData.cube.Stats);
                    vm.cube = result.data;
                });
            }
        }

        var selectedTotal = vm.filter(vm.enrichments, 'IsSelected', true),
            EligibleEnrichments = vm.filter(vm.enrichments, 'IsInternal', false),
            DisabledForSalesTeamTotal = vm.filter(EligibleEnrichments, 'AttributeFlagsMap.CompanyProfile.hidden', true),
            EnabledForSalesTeamTotal = EligibleEnrichments.length - DisabledForSalesTeamTotal.length;

        if(!vm.lookupMode) {
            DataCloudStore.setMetadata('generalSelectedTotal', selectedTotal.length);
            DataCloudStore.setMetadata('premiumSelectedTotal', vm.filter(selectedTotal, 'IsPremium', true).length);
            DataCloudStore.setMetadata('enabledForSalesTeamTotal', EnabledForSalesTeamTotal);
        }

        vm.generalSelectedTotal = DataCloudStore.getMetadata('generalSelectedTotal');
        vm.premiumSelectedTotal = DataCloudStore.getMetadata('premiumSelectedTotal');

        var timestamp5 = new Date().getTime();
        console.info('xhrResult();\t\t\t', '[' + (timestamp2 - timestamp) + ':' + (timestamp3 - timestamp2) + ':' + (timestamp5 - timestamp4) + ']\t ' + ((timestamp3 - timestamp) + (timestamp5 - timestamp4)) + 'ms\t concurrent:'+vm.concurrentIndex);
    }

    vm.generateTree = function(isComplete) {
        var timestamp = new Date().getTime();
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

            obj[category][subcategory].push(index);
        });

        var timestamp2 = new Date().getTime();
        console.info('generateTree();\t\t\t', (timestamp2 - timestamp) + 'ms');

        if (isComplete) {
            vm.categories.forEach(function(category, item) {
                if (obj[category]) {
                    getEnrichmentSubcategories(category, Object.keys(obj[category]));
                }
            });

            getTopAttributes();
            getHighlightMetadata();

            if(vm.metadataSegments || QueryRestriction) {
                getExplorerSegments(vm.enrichments);
                //console.log(vm.filter(vm.enrichments, 'FieldName', 'TechIndicator_AmazonSimpleDB'));
            }

            //console.log('vm.highlightMetadata:\t ', vm.highlightMetadata);
        }
    }

    var getEnrichmentSubcategories = function(category, subcategories) {
        vm._subcategories[category] = subcategories;
        vm.subcategories[category] = subcategories;

        if (subcategories.length <= 1) {
            vm.subcategoriesExclude.push(category);
        }

        vm.filterEmptySubcategories();
    }

    vm.removeFromArray = function(array, item) {
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
                addUniqueToArray(vm.subcategoriesExclude, vm.category);
                vm.setSubcategory(newCategories[0]);
                vm.updateStateParams();
            } else {
                if (vm.subcategoriesExclude.indexOf(vm.category)) {
                    vm.setSubcategory('');
                    vm.updateStateParams();
                }

                vm.removeFromArray(vm.subcategoriesExclude, vm.category);
            }

            vm.subcategories[vm.category] = newCategories;
        }
    }

    var addUniqueToArray = function(array, item) {
        if (array && item && !array.indexOf(item)) {
            array.push(item);
        }
    }

    var breakOnFirstEncounter = function(items, property, value, returnObj) {
        for (var i=0,item; i<items.length; i++) {
            if (value === null) {
                if(typeof items[i][property] !== 'undefined') {
                    if(returnObj) {
                        return items[i];
                    }
                    return true;
                }
            }
            if (typeof value === 'object') {
                if(typeof items[i][property] === 'object' && items[i][property] !== null) {
                    if(returnObj) {
                        return items[i];
                    }
                    return true;
                }
            }
            if (items[i][property] == value) {
                    if(returnObj) {
                        return items[i];
                    }
                return true;
            };
        }
        if(returnObj) {
            return null;
        }
        return false;
    }

    var getHighlightMetadata = function() {
        var timestamp = new Date().getTime();

        vm.categories.forEach(function(category) {
            if (vm.enrichmentsObj && vm.enrichmentsObj[category]) {
                vm.highlightMetadata.categories[category] = {};

                var items = vm.enrichmentsObj[category],
                    disabled = breakOnFirstEncounter(items, 'HighlightHidden', true),
                    enabled = breakOnFirstEncounter(items, 'HighlightHidden', false),
                    dirty = breakOnFirstEncounter(items, 'AttributeFlagsMap', {});

                vm.highlightMetadata.categories[category].enabled = enabled ? 1 : 0;
                vm.highlightMetadata.categories[category].disabled = disabled ? 1 : 0;
                vm.highlightMetadata.categories[category].dirty = dirty ? 1 : 0;

                if (vm.subcategories[category] && vm.subcategories[category].length > 1) {
                    vm.highlightMetadata.categories[category].subcategories = {};

                    vm.subcategories[category].forEach(function(subcategory){
                        vm.highlightMetadata.categories[category].subcategories[subcategory] = {};

                        var items = vm.filter(vm.enrichmentsObj[category], 'Subcategory', subcategory),
                            disabled = breakOnFirstEncounter(items, 'HighlightHidden', true),
                            enabled = breakOnFirstEncounter(items, 'HighlightHidden', false),
                            dirty = breakOnFirstEncounter(items, 'AttributeFlagsMap', {});

                        vm.highlightMetadata.categories[category].subcategories[subcategory].enabled = enabled ? 1 : 0;
                        vm.highlightMetadata.categories[category].subcategories[subcategory].disabled = disabled ? 1 : 0;
                        vm.highlightMetadata.categories[category].subcategories[subcategory].dirty = dirty ? 1 : 0;
                    });
                }
            }

        });

        var timestamp2 = new Date().getTime();
        console.info('getHighlightMetadata():\t ' + (timestamp2 - timestamp) + 'ms');
    }

    vm.highlightTypes = {
        enabled: 'Enabled for Sales Team',
        disabled: 'Disabled for Sales Team',
        highlighted: 'Highlight for Sales Team'
    }

    vm.highlightTypesCategory = {
        enabled: vm.highlightTypes.enabled,
        disabled: vm.highlightTypes.disabled
    }

    vm.showHighlighting = function() {
        return (vm.section == 'team' || vm.section == 'insights' || vm.section == 'lookup');
    }

    vm.highlightTypesCategoryLabel = function(category, subcategory) {
        var category = category || '',
            subcategory = subcategory || '',
            metadata = {},
            type = 'enabled',
            label = vm.highlightTypesCategory[type];

        if (category) {
            metadata = vm.highlightMetadata.categories[category];
            if(subcategory) {
                metadata = vm.highlightMetadata.categories[category].subcategories[subcategory];
            }
        }

        if (metadata) {
            if(metadata.enabled && metadata.disabled) {
                type = 'dirty';
                label = 'Some Enabled for Sales Team';
            } else if(!metadata.enabled) {
                type = 'disabled';
                label = vm.highlightTypesCategory[type];
            }
        }

        return {
            type: type,
            label: label
        };
    }

    var highlightOptionsInitState = function(enrichment) {
        var ret = {type: 'enabled', label: '', highlighted: false, enabled: false};

        if (!enrichment.AttributeFlagsMap || !enrichment.AttributeFlagsMap.CompanyProfile) {
            ret.type = 'enabled';
            ret.label = vm.highlightTypes[ret.type];
            ret.enabled = true;

            return ret;
        }

        ret.enabled = !enrichment.AttributeFlagsMap.CompanyProfile.hidden;
        ret.dirty = false;

        if (enrichment.AttributeFlagsMap.CompanyProfile.hidden === true) {
            ret.type = 'disabled';
            ret.enabled = false;
            ret.dirty = true;
        }

        if (enrichment.AttributeFlagsMap.CompanyProfile.hidden === false) {
            ret.type = 'enabled';
            ret.enabled = true;
            ret.dirty = true;
        }

        if (enrichment.AttributeFlagsMap.CompanyProfile.highlighted === true) {
            ret.type = 'highlighted';
            ret.highlighted = true;
            ret.enabled = true;
            ret.dirty = true;
        }

        if (ret.type) {
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
        if (type === enrichment.HighlightState.type) {
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

        if (type === 'highlighted') {
            flags.hidden = false;
            flags.highlighted = true;
        } else if (type === 'enabled') {
            flags.hidden = false;
        } else if (type === 'disabled') {
            flags.hidden = true;
            flags.highlighted = false;
        }

        vm.statusMessage(vm.label.saving_alert, {wait: 0});

        setFlags(opts, flags).then(function(){
            vm.statusMessage(vm.label.saved_alert, {type: 'saved'});
            vm.closeHighlighterButtons();

            enrichment.HighlightState = {type: type, label: label, enabled: !flags.hidden, highlighted: flags.highlighted};
            enrichment.HighlightHidden = flags.hidden;
            enrichment.HighlightHighlighted = flags.highlighted;

            vm.enrichments.find(function(i){return i.FieldName === enrichment.FieldName;}).AttributeFlagsMap = {
                CompanyProfile: flags
            };
            DataCloudStore.updateEnrichments(vm.enrichments);


            var EligibleEnrichments = vm.filter(vm.enrichments, 'IsInternal', false),
                DisabledForSalesTeamTotal = vm.filter(EligibleEnrichments, 'HighlightHidden', true),
                EnabledForSalesTeamTotal = EligibleEnrichments.length - DisabledForSalesTeamTotal.length;

            DataCloudStore.setMetadata('enabledForSalesTeamTotal', EnabledForSalesTeamTotal);
            getHighlightMetadata();
            vm.TileTableItems = {};
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
        },
        settings_changed = vm.highlightTypesCategoryLabel(category, subcategory).type !== type;

        if(!settings_changed) {
            return false;
        }

        if(!vm.highlightMetadata.categories[category][type]) {
            var changed = true;
        }

        if(type === 'disabled') {
            flags.highlighted = false;
        }


        vm.statusMessage(vm.label.saving_alert, {wait: 0});

        setFlagsByCategory(opts, flags).then(function(){
            vm.statusMessage(vm.label.saved_alert, {type: 'saved'});
            vm.closeHighlighterButtons();
            var enrichments = vm.filter(vm.enrichments, 'Category', category);

            if (subcategory) {
                enrichments = vm.filter(enrichments, 'Subcategory', subcategory);
            }

            for (var i in enrichments) {
                var enrichment = enrichments[i],
                flags = {},
                label = (enrichment.HighlightHighlighted ? enrichment.HighlightState.label : vm.highlightTypes[type]),
                wasHighlighted = enrichment.HighlightHighlighted,
                _type = type;

                if(!enrichment.AttributeFlagsMap) {
                    enrichment.AttributeFlagsMap = {};
                }
                if(!enrichment.AttributeFlagsMap.CompanyProfile) {
                    enrichment.AttributeFlagsMap.CompanyProfile = {};
                }

                if (type === 'disabled') {
                    label = vm.highlightTypes[type];
                    flags.highlighted = false;
                    flags.hidden = true;
                    enrichment.HighlightHighlighted = false;
                    enrichment.HighlightState.highlighted = false;
                    enrichment.AttributeFlagsMap.CompanyProfile.highlighted = false;
                }

                if (type === 'enabled') {
                    flags.hidden = false;

                    if (wasHighlighted) {
                        _type = 'highlighted';
                    }
                }

                enrichment.AttributeFlagsMap.CompanyProfile.hidden = flags.hidden;
                enrichment.HighlightHidden = flags.hidden;
                enrichment.HighlightState.type = _type;
                enrichment.HighlightState.label = label;
                enrichment.HighlightState.enabled = !flags.hidden;

                flags = {};
            }
            DataCloudStore.updateEnrichments(vm.enrichments);

            var EligibleEnrichments = vm.filter(vm.enrichments, 'IsInternal', false),
                DisabledForSalesTeamTotal = vm.filter(EligibleEnrichments, 'HighlightHidden', true),
                EnabledForSalesTeamTotal = EligibleEnrichments.length - DisabledForSalesTeamTotal.length;

            DataCloudStore.setMetadata('enabledForSalesTeamTotal', EnabledForSalesTeamTotal);
            getHighlightMetadata();
            vm.TileTableItems = {};
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
                return '$' + $filter('number')(parseInt(item, 10)); //ben look at this later, use vm.fitler
            case 'DATE':
                var date = new Date(parseInt(item, 10));
                var year = date.getFullYear().toString();
                var month = (date.getMonth() + 1).toString();
                month = (month.length >= 2) ? month : ('0' + month);
                var day = date.getDate().toString();
                day = (day.length >= 2) ? day : ('0' + day);
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

        DataCloudStore.getAllTopAttributes().then(function(result){
            var timestamp = new Date().getTime();

            Object.keys(EnrichmentTopAttributes).forEach(function(catKey) {
                var category = result[catKey]['SubCategories'];

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

            var timestamp2 = new Date().getTime();
            console.info('getTopAttributes();\t\t', timestamp2 - timestamp + 'ms');
        });
    }

    function swap(context, i, j) {
        var temp = context[i];
        context[i] = context[j]
        context[j] = temp;
    }

    var getEnrichmentCategories = function() {
        if (EnrichmentTopAttributes) {

            vm.categories = Object.keys(EnrichmentTopAttributes);

            if (vm.show_segmentation && vm.section == 'segment.analysis') {
                var topCategories = [
                    'Contact',
                    'Product',
                    'Firmographics',
                    'Intent',
                    'Technology Profile'
                ];

                topCategories.forEach(function(category, index) {
                    swap(vm.categories, vm.categories.indexOf(category), index);
                });
            } else {
                var removeCategories = [
                    'Product',
                    'Contact'
                ];

                removeCategories.forEach(function(category) {
                    vm.categories.splice(vm.categories.indexOf(category), 1);
                })
            }

            for (var i in vm.categories) {
                vm.categoryCounts[vm.categories[i]] = null;
            }

            vm.enable_category_dropdown = true;
        }
    }

    vm.getTileTableItems = function(category, subcategory, segment, limit, debug) {
        var items = [],
            limit = (limit === 0 ? 0 : null) || limit || null;

        if (!vm.TileTableItems[category]) {
            vm.TileTableItems[category] = {};
        }

        if (!vm.TileTableItems[category][(subcategory || 'all')]) {
            vm.TileTableItems[category][(subcategory || 'all')] = {};
        } else {
            return vm.TileTableItems[category][(subcategory || 'all')];
        }

        var timestamp = new Date().getTime();

        if (vm.topAttributes[category]) {
            var timestamp_a = new Date().getTime();

            if (!subcategory && vm.isYesNoCategory(category, true)) {
                Object.keys(vm.topAttributes[category].SubCategories).forEach(function(key, index) {
                    items = items.concat(vm.topAttributes[category].SubCategories[key]);
                });
            } else if(!subcategory) {
                items = vm.topAttributes[category].SubCategories['Other'];
            } else {
                items = vm.topAttributes[category].SubCategories[subcategory];
            }
            
            var timestamp_b = new Date().getTime();

            if (!vm.lookupMode && items) {
                items.forEach(function(item, itemKey) {
                    var index = vm.enrichmentsMap[item.Attribute],
                        enrichment = vm.enrichments[index],
                        map = [
                            //'Value',
                            //'AttributeValue',
                            'FundamentalType',
                            'DisplayName',
                            'Subcategory',
                            'IsSelected',
                            'IsPremium',
                            'IsInternal',
                            'ImportanceOrdering',
                            'HighlightHidden',
                            'HighlightHighlighted',
                            'SegmentChecked'
                        ];

                    if (enrichment) { 
                        map.forEach(function(key){
                            item[key] = enrichment[key];
                        });

                        enrichment.NonNullCount = item.NonNullCount;
                        item.Hide = false;
                    }
                    if(!vm.searchFields(enrichment)) {
                        item.Hide = true;
                    }
                });
            }
            var timestamp_c = new Date().getTime();
        }

        var timestamp2 = new Date().getTime();

        if (vm.lookupMode || !items || items.length == 0) {
            items = vm.enrichmentsObj[category];

            if (subcategory || vm.isYesNoCategory(category, true)) {
                items = items.filter(function(item) {
                    var isSubcategory = subcategory ? item.Subcategory == subcategory : true,
                        attrValue = (vm.lookupFiltered ? vm.lookupFiltered[item.FieldName] : item.AttributeValue);

                    if (vm.lookupMode && attrValue && isSubcategory) {
                        item.Value = attrValue;
                        if (!vm.metadata.toggle.show.nulls && attrValue == 'No') {
                            return false;
                        } else {
                            return true;
                        }
                    }

                    return isSubcategory;
                });
            }
        }

        var timestamp3 = new Date().getTime();
        var _items = {};

        /**
         * if we aren't showing highlighted items don't segment them even if the UI asks
         */
        if(segment === 'HighlightHighlighted' && !vm.showHighlighting()) {
            segment = '';
        }

        if (segment && items) {
            var segmented = vm.filter(items, segment, true),
                other = vm.filter(items, segment, false);

            _items[segment] = segmented;
        }

        _items['other'] = other || items;
        items = _items;

        vm.TileTableItems[category][(subcategory || 'all')] = items;

        var timestamp4 = new Date().getTime(),
            a = (timestamp_b - timestamp_a),
            b = (timestamp_c - timestamp_b),
            c = (timestamp3 - timestamp2),
            d = (timestamp4 - timestamp3);

        console.info(
            'getTileTableItems();\t',
            '[' + (isNaN(a) ? '' : a + ':') + (isNaN(b) ? '' : b + ':') + c + ':' + d + ']\t '+
            (timestamp4 - timestamp) + 'ms\t',
            category, '\t',
            subcategory, '\t',
            items
        );

        return items;
    }

    vm.generateTileTableLabel = function(items) {
        return items
            ? 'Top ' + (items.length > 1 ? items.length + ' attributes' : 'attribute')
            : '';
    }

    vm.inCategory = function(enrichment){
        if (enrichment.DisplayName && !(_.size(vm.selected_categories))) { // for case where this is used as a | filter in the enrichments ngRepeat on initial state
            return true;
        }

        var selected = (typeof vm.selected_categories[enrichment.Category] === 'object');

        return selected;
    };

    vm.selectEnrichment = function(enrichment){
        vm.saveDisabled = 0;
        vm.selectDisabled = 0;
        var selectedTotal = vm.filter(vm.enrichments, 'IsSelected', true);

        if (enrichment.IsPremium) {
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

            if (!enrichment.WasDirty) {
                enrichment.WasDirty = true;
                var notselected = vm.filter(vm.enrichments, 'IsSelected', false);
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

        if (wait) {
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

        for (var i in selectedObj) {
            if(selectedObj[i].FieldName) {
                selected.push(selectedObj[i].FieldName);
            }
        }

        for (var i in deselectedObj) {
            if(deselectedObj[i].FieldName) {
                deselected.push(deselectedObj[i].FieldName);
            }
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

            if (selectedObj.length > 0 || deselectedObj.length > 0) {
                var dirtyObj = vm.filter(vm.enrichments, 'IsDirty', true);

                for (var i in dirtyObj){
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
        };

        return fieldTypes[fieldType] || fieldTypes.default;
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
        if(categories.length <= 1 && !vm.lookupMode) {
            vm.setCategory(categories[0]);
        }
    }

    vm.isYesNoCategory = function(category, includeKeywords) {
        var list = ['Website Profile','Technology Profile'];

        if (includeKeywords) {
            list.push('Website Keywords');
            list.push('Product');
        }

        return list.indexOf(category) >= 0;
    }

    vm.percentage = function(number, total, suffix, limit) {
        var suffix = suffix || '',
            percentage = 0;

        if (number && total) {
            percentage = ((number / total) * 100);

            if (typeof limit != 'undefined') {
                percentage = percentage.toFixed(limit);
            }
            return percentage + suffix;
        }

        return 0;
    }

    var getEnrichmentCube = function() {
        return DataCloudStore.getCube();
    }

    vm.makeSegmentsRangeKey = function(enrichment, range){
        var fieldName = enrichment.Attribute || enrichment.FieldName,
            key = fieldName + (range ? Object.values(range).join('') : '');
        return key;
    }

    var getExplorerSegments = function(enrichments) {
        vm.clearExplorerSegments();
        var metadataSegments = vm.metadataSegments || QueryRestriction;
        for(var i in metadataSegments) {
            var restrictions = metadataSegments[i];
            for(var i in restrictions) {
                var item = restrictions[i];
                if(item.bucketRestriction) {
                    var restriction = item.bucketRestriction,
                        key = restriction.lhs.columnLookup.column_name,
                        range = restriction.range,
                        enrichment = breakOnFirstEncounter(vm.enrichments, 'FieldName', key, true),
                        fieldName = enrichment.FieldName,
                        category = enrichment.Category,
                        index = vm.enrichmentsMap[fieldName];
                        if(index || index === 0) {
                            vm.enrichments[index].SegmentChecked = true;
                            vm.enrichments[index].SegmentRangesChecked = {};
                            vm.segmentAttributeInput[vm.enrichments[index].FieldName] = true;
                            vm.segmentAttributeInputRange[vm.makeSegmentsRangeKey(enrichment, range)] = true;
                        }
                }
            }
        }
    }

    vm.clearExplorerSegments = function() { 
        var _enrichments = vm.filter(vm.enrichments, 'SegmentChecked', true);
        vm.segmentAttributeInput = {};
        _enrichments.forEach(function(enrichment){
            index = vm.enrichmentsMap[enrichment.FieldName];
            if(index || index === 0) {
                delete vm.enrichments[index].SegmentChecked;
            }
        });
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
            if (enrichment.DisplayName && textSearch(enrichment.DisplayName, vm.query)) {
                return true;
            } else if (enrichment.Description && textSearch(enrichment.Description, vm.query)) {
                return true;
            } else {
                return false;
            }
        }

        return true;
    }

    vm.subcategoryCount = function(category, subcategory) {
        var filtered = vm.enrichmentsObj[category];

        vm.hasCategoryCount = 0;

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

        vm.hasCategoryCount = result.length;
        return result.length;
    }
    
    vm.segmentAttributeInput = DataCloudStore.getMetadata('segmentAttributeInput') || {};
    vm.selectSegmentAttribute = function(attribute) {
        if(!vm.cube.Stats) {
            return false;
        }
        var attributeKey = attribute.Attribute || attribute.FieldName,
            stat = vm.getAttributeStat(attribute) || {},
            attributeRangeKey = (stat.Range ? vm.makeSegmentsRangeKey(attribute, stat.Range) : '');

        vm.segmentAttributeInput[attributeKey] = !vm.segmentAttributeInput[attributeKey];
        DataCloudStore.setMetadata('segmentAttributeInput', vm.segmentAttributeInput);
        if(attributeRangeKey) {
            vm.segmentAttributeInputRange[attributeRangeKey] = !vm.segmentAttributeInputRange[attributeRangeKey];
        }
        vm.saveSegmentEnabled = true;

        if (vm.segmentAttributeInput[attributeKey] === true) {
            QueryStore.addRestriction({columnName: attributeKey, range: stat.Range});
        } else {
            QueryStore.removeRestriction({columnName: attributeKey, range: stat.Range});
        }
    }

    vm.segmentAttributeInputRange = vm.segmentAttributeInputRange || {};
    vm.selectSegmentAttributeRange = function(enrichment, stat, disable) {
        var disable = disable || false,
            attributeKey = enrichment.Attribute || enrichment.FieldName,
            attributeRangeKey = vm.makeSegmentsRangeKey(enrichment, stat.Range),
            fieldName = enrichment.FieldName;
        if(disable) {
            return false;
        }

        vm.segmentAttributeInput[attributeKey] = !vm.segmentAttributeInput[attributeKey];
        vm.segmentAttributeInputRange[attributeRangeKey] = !vm.segmentAttributeInputRange[attributeRangeKey];
        vm.saveSegmentEnabled = true;

        //vm.enrichments[index].SegmentChecked = true;

        if (vm.segmentAttributeInputRange[attributeRangeKey] === true) {
            QueryStore.addRestriction({columnName: fieldName, range: stat.Range});
        } else {
            QueryStore.removeRestriction({columnName: fieldName, range: stat.Range});
        }
        /*
         * Rebuild the tile table items
         */
        vm.TileTableItems = {};
        if(vm.metadataSegments || QueryRestriction) {
            getExplorerSegments(vm.enrichments);
        }
    }

    var getSegmentBucketInputs = function() {
        var buckets = {},
            metadataSegments = vm.metadataSegments || QueryRestriction;
        if(metadataSegments && metadataSegments.all) {
            metadataSegments.all.forEach(function(item){
                var bucketId = item.bucketRestriction.lhs.columnLookup.column_name + item.bucketRestriction.range.max;

                buckets[bucketId] = true;
            });
        }
        return buckets;
    }

    vm.segmentBucketInput = getSegmentBucketInputs();
    vm.selectBucketInput = function(id, bucket) {
        var bucketId = id + bucket,
            range = {min: bucket, max: bucket, is_null_only: false};

        vm.segmentBucketInput[bucketId] = !vm.segmentBucketInput[bucketId];
        vm.saveSegmentEnabled = true;
        if (vm.segmentBucketInput[bucketId] === true) {
            QueryStore.addRestriction({columnName: id, range: range});
        } else {
            QueryStore.removeRestriction({columnName: id, range: range});
        }

    }

    vm.inModel = function() {
        var name = $state.current.name.split('.');
        return name[1] == 'model';
    }

    vm.refineQuery = function() {
        if (vm.inModel()) {
            $state.go('home.model.analysis.explorer.query');
        } else{
            $state.go('home.segment.explorer.query');
        }
    }

    vm.saveSegment = function() {
        if(Object.keys(vm.segmentAttributeInput).length || Object.keys(vm.segmentAttributeInputRange).length) {
            SegmentServiceProxy.CreateOrUpdateSegment().then(function(result) {
                if (!result.errorMsg) {
                    if (vm.inModel()) {
                        $state.go('home.model.segmentation', {}, {notify: true})
                    } else {
                        $state.go('home.segments', {}, {notify: true})
                    }
                }
            });
        }
    }

    vm.init();
})
.directive('fallbackSrc', function () {
    var fallbackSrc = {
        link: function postLink(scope, iElement, iAttrs) {
            iElement.bind('error', function() {
                angular.element(this).attr("src", iAttrs.fallbackSrc);
                angular.element(this).css({display: 'initial'}); // removes onerror hiding image
            });
        }
    }

    return fallbackSrc;
});
