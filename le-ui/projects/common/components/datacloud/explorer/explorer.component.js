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
    $scope, $filter, $timeout, $interval, $window, $document, $q, $state, $stateParams, Enrichments,
    ApiHost, BrowserStorageUtility, ResourceUtility, FeatureFlagService, DataCloudStore, DataCloudService,
    EnrichmentTopAttributes, EnrichmentPremiumSelectMaximum, LookupStore, QueryService, QueryStore,
    SegmentService, SegmentStore, QueryRestriction, CurrentConfiguration, EnrichmentCount, LookupResponse 
){
    var vm = this,
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
            saved_error: 'Your changes could not be saved.',
            saving_alert: 'Your changes are being saved. <i class="fa fa-cog fa-spin fa-fw"></i>',
            changed_alert: 'No changes will be saved until you press the \'Save\' button.',
            disabled_alert: 'You have disabled an attribute.'
        },
        highlightMetadata: {
            categories: {}
        },
        EnrichmentPremiumSelectMaximum: EnrichmentPremiumSelectMaximum,
        lookupMode: (LookupResponse && LookupResponse.attributes !== null),
        lookupFiltered: LookupResponse.attributes,
        LookupResponse: LookupStore.response,
        no_lookup_results_message: false,
        hasCompanyInfo: (LookupStore.response && LookupStore.response.companyInfo ? Object.keys(LookupStore.response.companyInfo).length : 0),
        count: (LookupResponse.attributes ? Object.keys(LookupResponse.attributes).length : EnrichmentCount),        
        show_internal_filter: FeatureFlagService.FlagIsEnabled(flags.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES) && $stateParams.section != 'insights' && $stateParams.section != 'team',
        show_lattice_insights: FeatureFlagService.FlagIsEnabled(flags.LATTICE_INSIGHTS),
        show_segmentation: FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL),
        enabledManualSave: false,
        enrichmentsObj: {}, // by Category
        enrichmentsMap: {}, // by ColumnId, value is enrichments[] index
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
        metadata: DataCloudStore.metadata,
        authToken: BrowserStorageUtility.getTokenDocument(),
        userSelectedCount: 0,
        selectDisabled: 1,
        saveDisabled: 1,
        saveSegmentEnabled: false,
        selectedCount: 0,
        section: $stateParams.section,
        category: $stateParams.category,
        subcategory: $stateParams.subcategory,
        openHighlighter: {},
        categoryCounts: {},
        TileTableItems: {},
        workingBuckets: CurrentConfiguration,
        pagesize: 24,
        categorySize: 7,
        addBucketTreeRoot: null,
        feedbackModal: DataCloudStore.getFeedbackModal(),
        stateParams: $stateParams
    });

    DataCloudStore.setMetadata('lookupMode', vm.lookupMode);

    vm.init = function() {
        if (vm.lookupMode && vm.LookupResponse.errorCode) {
            $state.go('home.datacloud.explorer');
        }

        DataCloudStore.getCube().then(function(result) {
            vm.cube = result;
        });

        vm.processCategories();
        vm.processEnrichments(Enrichments, true);
        vm.generateTree(true);

        if (vm.lookupMode && typeof vm.lookupFiltered === 'object' && Object.keys(vm.lookupFiltered).length < 1) {
            vm.no_lookup_results_message = true;
        }

        if (vm.section === 'segment.analysis') {
            vm.setCurrentRestrictionForSaveButton();
            vm.metadataSegments = QueryRestriction;
        }

        // for Advanced Query Builder
        vm.addBucketTreeRoot = QueryStore.getAddBucketTreeRoot();

        $scope.$on("$destroy", function() {
            delete (vm.addBucketTreeRoot = QueryStore.setAddBucketTreeRoot(null));
        });

        DataCloudStore.setFeedbackModal(false);
    }

    /* some rules that might hide the page */
    vm.hidePage = function() {
        if (vm.lookupMode && typeof vm.lookupMode === 'object' && Object.keys(vm.lookupFiltered).length < 1) {
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

    vm.relanding = function() {

        vm.TileTableItems = {};

        if (vm.metadataSegments || QueryRestriction) {
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
            // console.log('prop:', property, 'value:', value, 'items:', items, 'result:', result);
        }

        return result;
    }

    vm.closeHighlighterButtons = function(index) {
        var index = index || '';

        for (var i in vm.openHighlighter) {
            if (vm.openHighlighter[i].open) {
                vm.openHighlighter[i].open = false;

                if (!index) {
                    if (!$scope.$$phase) { // this is bad but short term hack
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

    vm.processEnrichments = function(enrichments) {
        if (vm.lookupFiltered !== null) {
            for (var i=0, _enrichments=[]; i<enrichments.length; i++) {
                if (vm.lookupFiltered && vm.lookupFiltered[enrichments[i].ColumnId]) {
                    _enrichments.push(enrichments[i]);
                }
            }
        } else {
            var _enrichments = enrichments;
        }

        for (var i=0, enrichment; i<_enrichments.length; i++) {
            enrichment = _enrichments[i];

            if (!enrichment) {
                continue;
            }
            
            if (!vm.enrichmentsObj[enrichment.Category]) {
                vm.enrichmentsObj[enrichment.Category] = [];
            }

            if (enrichment.IsInternal !== true) {
                enrichment.IsInternal = false;
            }

            vm.enrichmentsMap[enrichment.ColumnId] = vm.enrichments.length;
            vm.enrichmentsObj[enrichment.Category].push(enrichment);
            vm.enrichments.push(enrichment);
        }

        DataCloudStore.setEnrichments(vm.enrichments);

        vm.hasSaved = vm.filter(vm.enrichments, 'IsDirty', true).length;

        var selectedTotal = vm.filter(vm.enrichments, 'IsSelected', true),
            EligibleEnrichments = vm.filter(vm.enrichments, 'IsInternal', false),
            DisabledForSalesTeamTotal = vm.filter(EligibleEnrichments, 'AttributeFlagsMap.CompanyProfile.hidden', true),
            EnabledForSalesTeamTotal = EligibleEnrichments.length - DisabledForSalesTeamTotal.length;

        if (vm.lookupMode) {
            LookupStore.add('count', vm.enrichments.length);
        } else {
            DataCloudStore.setMetadata('generalSelectedTotal', selectedTotal.length);
            DataCloudStore.setMetadata('premiumSelectedTotal', vm.filter(selectedTotal, 'IsPremium', true).length);
            DataCloudStore.setMetadata('enabledForSalesTeamTotal', EnabledForSalesTeamTotal);
        }

        vm.generalSelectedTotal = DataCloudStore.getMetadata('generalSelectedTotal');
        vm.premiumSelectedTotal = DataCloudStore.getMetadata('premiumSelectedTotal');
    }

    vm.generateTree = function(isComplete) {
        var timestamp = new Date().getTime();
        var obj = {};

        vm.enrichments.forEach(function(item, index) {
            var category = item.Category;
            var subcategory = item.Subcategory;

            if (!obj[category]) {
                obj[category] = {};
            }

            if (!obj[category][subcategory]) {
                obj[category][subcategory] = [];
            }

            if (vm.lookupMode && vm.lookupFiltered[item.ColumnId]) {
                item.AttributeValue = vm.lookupFiltered[item.ColumnId];
            }

            item.HighlightState = highlightOptionsInitState(item);
            item.HighlightHidden = (item.AttributeFlagsMap && item.AttributeFlagsMap.CompanyProfile && item.AttributeFlagsMap.CompanyProfile.hidden === true ? true : false);
            item.HighlightHighlighted = (item.AttributeFlagsMap && item.AttributeFlagsMap.CompanyProfile && item.AttributeFlagsMap.CompanyProfile.highlighted ? item.AttributeFlagsMap.CompanyProfile.highlighted : false);

            item.IsRatingEngineAttribute = ratingsEngineAttributeState(item);

            obj[category][subcategory].push(index);
        });

        var timestamp2 = new Date().getTime();
        // console.info('generateTree();\t\t\t', (timestamp2 - timestamp) + 'ms');

        if (isComplete) {
            vm.categories.forEach(function(category, item) {
                if (obj[category]) {
                    getEnrichmentSubcategories(category, Object.keys(obj[category]));
                }
            });

            getTopAttributes();
            getHighlightMetadata();

            if (vm.metadataSegments || QueryRestriction) {
                getExplorerSegments(vm.enrichments);
            }
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
                if (typeof items[i][property] !== 'undefined') {
                    if (returnObj) {
                        return items[i];
                    }

                    return true;
                }
            }

            if (typeof value === 'object') {
                if (typeof items[i][property] === 'object' && items[i][property] !== null) {
                    if (returnObj) {
                        return items[i];
                    }

                    return true;
                }
            }

            if (items[i][property] == value) {
                if (returnObj) {
                    return items[i];
                }

                return true;
            };
        }

        if (returnObj) {
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
        // console.info('getHighlightMetadata():\t ' + (timestamp2 - timestamp) + 'ms');
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

    var ratingsEngineAttributeState = function(item) {
        var selected = DataCloudStore.getRatingEngineAttributes();
        if(selected.indexOf(item.ColumnId) >= 0) {
            return true;
        }
        return false;
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
                fieldName: enrichment.ColumnId
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

            vm.enrichments.find(function(i){return i.ColumnId === enrichment.ColumnId;}).AttributeFlagsMap = {
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
            case 'percentage':
                var percentage = Math.round(item * 100);
                return percentage !== NaN ? percentage + '%' : '';
            case 'numeric':
                return $filter('number')(parseInt(item, 10));
            case 'currency':
                return '$' + $filter('number')(parseInt(item, 10)); //ben look at this later, use vm.fitler
            case 'date':
                var date = new Date(parseInt(item, 10));
                var year = date.getFullYear().toString();
                var month = (date.getMonth() + 1).toString();
                month = (month.length >= 2) ? month : ('0' + month);
                var day = date.getDate().toString();
                day = (day.length >= 2) ? day : ('0' + day);
                return '' + year + month + day;
            case 'uri':
            case 'alpha':
            case 'boolean':
            case 'enum':
            case 'email':
            case 'phone':
            case 'year':
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

            Object.keys(EnrichmentTopAttributes).forEach(function(catKey, catItem) {
                var category = EnrichmentTopAttributes[catKey]['Subcategories'];

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
            // console.info('getTopAttributes();\t\t', timestamp2 - timestamp + 'ms');
        });
    }

    function swap(context, i, j) {
        var temp = context[i];
        context[i] = context[j]
        context[j] = temp;
    }

    vm.processCategories = function() {
        vm.categories = Object.keys(EnrichmentTopAttributes).sort();

        if (vm.show_segmentation && vm.section == 'segment.analysis') {
            var topCategories = [
                'Contact',
                'Product',
                'Firmographics',
                'Intent',
                'Technology Profile'
            ];

            topCategories.forEach(function(category, index) {
                if (vm.categories.indexOf(category) >= 0) {
                    swap(vm.categories, vm.categories.indexOf(category), index);
                }
            });
        } else {
            var removeCategories = [
                'Product',
                'Contact',
                'Engagement'
            ];

            removeCategories.forEach(function(category) {
                vm.categories.slice(vm.categories.indexOf(category), 1);
            });
        }

        for (var i in vm.categories) {
            vm.categoryCounts[vm.categories[i]] = null;
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
                Object.keys(vm.topAttributes[category].Subcategories).forEach(function(key, index) {
                    items = items.concat(vm.topAttributes[category].Subcategories[key]);
                });
            } else if(!subcategory) {
                items = vm.topAttributes[category].Subcategories['Other'];
            } else {
                items = vm.topAttributes[category].Subcategories[subcategory];
            }
            
            var timestamp_b = new Date().getTime();

            if (items) {
                items.forEach(function(item, itemKey) {

                    // console.log(item);

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


                    // console.log(enrichment);

                    if (enrichment) {
                        if(!vm.lookupMode) {
                            map.forEach(function(key){
                                


                                item[key] = enrichment[key];



                            });

                            enrichment.Count = item.Count;
                        }
                        item.Hide = false;
                        if(!vm.searchFields(enrichment)) {
                            item.Hide = true;
                        }
                        enrichment.Hide = item.Hide;
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
                        attrValue = (vm.lookupFiltered ? vm.lookupFiltered[item.ColumnId] : item.AttributeValue);

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

        // console.info(
        //     'getTileTableItems();\t',
        //     '[' + (isNaN(a) ? '' : a + ':') + (isNaN(b) ? '' : b + ':') + c + ':' + d + ']\t '+
        //     (timestamp4 - timestamp) + 'ms\t',
        //     category, '\t',
        //     subcategory, '\t',
        //     items, '\t',
        //     {
        //         'vm.enrichmentsFilter': vm.enrichmentsFilter(), 
        //         'vm.metadata.toggle': vm.metadata.toggle
        //     }
        // );

        return items;
    }

    vm.filterHideTrue = function(item) {
        if (item.Hide === true) {
            return false;
        } else {
            return true;
        }

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
            wait = (opts.wait || opts.wait === 0 ? opts.wait : 1500),
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
            if(selectedObj[i].ColumnId) {
                selected.push(selectedObj[i].ColumnId);
            }
        }

        for (var i in deselectedObj) {
            if(deselectedObj[i].ColumnId) {
                deselected.push(deselectedObj[i].ColumnId);
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
            if(result.errorCode) {
                vm.statusMessage(vm.label.saved_error, {type: 'error'});
            } else {
                vm.statusMessage(vm.label.saved_alert, {type: 'saved'});
            }
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
        
        if (vm.categoryCounts) {
            for (var i in vm.categoryCounts) {
                if (vm.categoryCounts[i] > 0) {
                    categories.push(i);
                }
            }
        }

        if(categories.length <= 1 && !vm.lookupMode) {
            vm.setCategory(categories[0]);
        }
    }

    vm.setCategory = function(category) {
        vm.category = category;
        DataCloudStore.setMetadata('category', category);
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

    var ObjectValues = function(obj) {
        var ar = [];
        if(obj && typeof obj ==='object') {
            for(var i in obj) {
                ar.push(obj[i]);
            }
        }
        return ar;
    }

    vm.makeSegmentsRangeKey = function(enrichment, range, label){

        var fieldName = enrichment.Attribute || enrichment.ColumnId,
            values = ObjectValues(range),
            key = fieldName + (range ? values.join('') : label);
        return key;
    }

    var getExplorerSegments = function(enrichments) {
        vm.clearExplorerSegments();

        if (vm.metadataSegment != undefined){
            var accountRestrictions = vm.metadataSegments.accountRestrictions,
                contactRestrictions = vm.metadataSegments.contactRestrictions;
        } else {
            var queryRestriction = QueryRestriction,
                accountRestrictions = queryRestriction.accountRestrictions,
                contactRestrictions = queryRestriction.contactRestrictions;
        };

        if (vm.addBucketTreeRoot) {
            return;
        }

        console.log(accountRestrictions, contactRestrictions);

        // FIXME: this should be recursive... -Lazarus
        for (var i=0; i < accountRestrictions.length; i++) {
            var restriction = accountRestrictions[i].restriction.logicalRestriction.restrictions;

            if (restriction.restriction) {
                var restriction = restriction.restriction,
                    range = restriction.bkt.Rng,
                    label = restriction.bkt.Lbl,
                    key = restriction.attr.split(".")[1],
                    enrichment = breakOnFirstEncounter(vm.enrichments, 'ColumnId', key, true),
                    index = vm.enrichmentsMap[key];

                if (index || index === 0) {
                    // vm.enrichments[index].SegmentChecked = true; PLS-4589
                    vm.enrichments[index].SegmentRangesChecked = {};
                    vm.segmentAttributeInput[vm.enrichments[index].ColumnId] = true;
                    vm.segmentAttributeInputRange[vm.makeSegmentsRangeKey(enrichment, range, label)] = true;
                }
            }
        }

        for (var i=0; i < contactRestrictions.length; i++) {
            var restriction = contactRestrictions[i].restriction.logicalRestriction.restrictions;

            if (restriction.restriction) {
                var restriction = restriction.restriction,
                    range = restriction.bkt.Rng,
                    label = restriction.bkt.Lbl,
                    key = restriction.attr.split(".")[1],
                    enrichment = breakOnFirstEncounter(vm.enrichments, 'ColumnId', key, true),
                    index = vm.enrichmentsMap[key];

                if (index || index === 0) {
                    // vm.enrichments[index].SegmentChecked = true; PLS-4589
                    vm.enrichments[index].SegmentRangesChecked = {};
                    vm.segmentAttributeInput[vm.enrichments[index].ColumnId] = true;
                    vm.segmentAttributeInputRange[vm.makeSegmentsRangeKey(enrichment, range, label)] = true;
                }
            }
        }
        
    }

    vm.clearExplorerSegments = function() {
        var _enrichments = vm.filter(vm.enrichments, 'SegmentChecked', true);

        vm.segmentAttributeInput = {};
        _enrichments.forEach(function(enrichment){
            delete enrichment.SegmentChecked;
        });

        // console.log(_enrichments);

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
        
        if($stateParams.gotoNonemptyCategory) {
            gotoNonemptyCategory();
        }

        vm.hasCategoryCount = result.length;
        return result.length;
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


    vm.segmentAttributeInput = DataCloudStore.getMetadata('segmentAttributeInput') || {};
    vm.selectSegmentAttribute = function(attribute) {
        if(!vm.cube.data.Stats) {
            return false;
        }

        console.log(attribute);

        var attributeKey = attribute.Attribute || attribute.FieldName,
            stat = vm.getAttributeStat(attribute) || {};

        // if (!stat.Rng) {
        //     return false;
        // }
        
        var attributeRangeKey = (stat.Lbl ? vm.makeSegmentsRangeKey(attribute, stat.Rng, stat.Lbl) : ''),
            index = vm.enrichmentsMap[attributeKey],
            enrichment = vm.enrichments[index],
            entity = enrichment.Entity,
            topBkt = attribute.TopBkt,
            segmentName = $stateParams.segment;

        vm.segmentAttributeInput[attributeKey] = !vm.segmentAttributeInput[attributeKey];
        DataCloudStore.setMetadata('segmentAttributeInput', vm.segmentAttributeInput);

        if(attributeRangeKey) {
            vm.segmentAttributeInputRange[attributeRangeKey] = !vm.segmentAttributeInputRange[attributeRangeKey];
        }

        QueryStore.counts.accounts.loading = true;
        QueryStore.counts.contacts.loading = true;

        if(entity === 'Account') {
            if (vm.segmentAttributeInput[attributeKey] === true) {
                QueryStore.addAccountRestriction({columnName: attributeKey, resourceType: entity, bkt: topBkt});
            } else {
                QueryStore.removeAccountRestriction({columnName: attributeKey, resourceType: entity, bkt: topBkt});
            }
        } else {
            if (vm.segmentAttributeInput[attributeKey] === true) {
                QueryStore.addContactRestriction({columnName: attributeKey, resourceType: entity, bkt: topBkt});
            } else {
                QueryStore.removeContactRestriction({columnName: attributeKey, resourceType: entity, bkt: topBkt});
            }
        }

        if (segmentName === 'Create') {
            vm.setCurrentRestrictionForSaveButton(); 
        } else {
            vm.checkSaveButtonState();
        }

    }

    vm.segmentAttributeInputRange = vm.segmentAttributeInputRange || {};
    vm.selectSegmentAttributeRange = function(enrichment, stat, disable) {
        
        var disable = disable || false,
            attributeKey = enrichment.Attribute || enrichment.ColumnName,
            attributeRangeKey = vm.makeSegmentsRangeKey(enrichment, stat.Rng, stat.Lbl),
            fieldName = enrichment.ColumnName,
            entity = enrichment.Entity,
            segmentName = $stateParams.segment;


        console.log(attributeRangeKey);

        //     if(stat.Rng === undefined){
        //         var attributeRangeKey = stat.Lbl;
        //     } else {
        //         var attributeRangeKey = vm.makeSegmentsRangeKey(enrichment, stat.Rng);
        //     }

        console.log(enrichment);

        if(disable) {
            return false;
        }
        if(entity === 'Transaction'){
            var entity = 'Account';
        }

        vm.segmentAttributeInput[attributeKey] = !vm.segmentAttributeInput[attributeKey];
        vm.segmentAttributeInputRange[attributeRangeKey] = !vm.segmentAttributeInputRange[attributeRangeKey];
        

        if(entity === 'Account') {
            if (vm.segmentAttributeInputRange[attributeRangeKey] === true) {
                QueryStore.addAccountRestriction({columnName: attributeKey, resourceType: entity, bkt: stat});
            } else {
                QueryStore.removeAccountRestriction({columnName: attributeKey, resourceType: entity, bkt: stat});
            }
        } else {
            if (vm.segmentAttributeInputRange[attributeRangeKey] === true) {
                QueryStore.addContactRestriction({columnName: attributeKey, resourceType: entity, bkt: stat});
            } else {
                QueryStore.removeContactRestriction({columnName: attributeKey, resourceType: entity, bkt: stat});
            }
        }


        vm.TileTableItems = {};
        if(vm.metadataSegments || QueryRestriction) {
            getExplorerSegments(vm.enrichments);
        }

        if (segmentName === 'Create') {
            vm.setCurrentRestrictionForSaveButton(); 
        } else {
            vm.checkSaveButtonState();
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
            QueryStore.addAccountRestriction({columnName: id, range: range});
        } else {
            QueryStore.removeAccountRestriction({columnName: id, range: range});
        }

    }

    vm.inModel = function() {
        var name = $state.current.name.split('.');
        return name[1] == 'model';
    }

    vm.refineQuery = function() {
        if (vm.inModel()) {
            $state.go('home.model.analysis.explorer.query.advanced');
        } else{
            $state.go('home.segment.explorer.query.advanced');
        }
    }

    vm.setCurrentRestrictionForSaveButton = function(){
        var segmentName = $stateParams.segment;

        if (segmentName === 'Create') {

            var accountRestriction = JSON.stringify({
                "restriction": {
                    "logicalRestriction": {
                        "operator": "AND",
                        "restrictions": []
                    }
                }
            });
            var contactRestriction = JSON.stringify({
                "restriction": {
                    "logicalRestriction": {
                        "operator": "AND",
                        "restrictions": []
                    }
                }
            });

            $stateParams.defaultSegmentRestriction = accountRestriction + contactRestriction;
            
            vm.checkSaveButtonState();

        } else {

            SegmentStore.getSegmentByName(segmentName).then(function(result) {

                var accountRestriction = JSON.stringify(result.account_restriction),
                    contactRestriction = JSON.stringify(result.contact_restriction);
                
                $stateParams.defaultSegmentRestriction = accountRestriction + contactRestriction;
                
                vm.checkSaveButtonState();
            });
        };
        
    };

    vm.checkSaveButtonState = function(){

        var oldVal = $stateParams.defaultSegmentRestriction,
            newAccountVal = JSON.stringify(QueryStore.getAccountRestriction()),
            newContactVal = JSON.stringify(QueryStore.getContactRestriction()),
            newVal = newAccountVal + newContactVal;

        if(oldVal === newVal){
            vm.saveSegmentEnabled = false;
        } else {
            vm.saveSegmentEnabled = true;
        };

    };

    vm.saveSegment = function() {
        if (Object.keys(vm.segmentAttributeInput).length || Object.keys(vm.segmentAttributeInputRange).length) {
            var segmentName = $stateParams.segment,
                ts = new Date().getTime();
            if (segmentName === 'Create') {
                var accountRestriction = QueryStore.getAccountRestriction(),
                    contactRestriction = QueryStore.getContactRestriction(),
                    segment = SegmentStore.sanitizeSegment({
                        'name': 'segment' + ts,
                        'display_name': 'segment' + ts,
                        'account_restriction': accountRestriction,
                        'contact_restriction': contactRestriction,
                        'page_filter': {
                            'row_offset': 0,
                            'num_rows': 10
                        }
                    });

                // console.log('saveSegment new', segmentName, ts, segment);

                SegmentService.CreateOrUpdateSegment(segment).then(function(result) {
                    
                    vm.saveSegmentEnabled = false;
                    $state.go('.', { segment: 'segment' + ts }, { notify: false });
                    vm.saved = true;

                });
            } else {
                SegmentStore.getSegmentByName(segmentName).then(function(result) {
                    var segmentData = result,
                        accountRestriction = QueryStore.getAccountRestriction(),
                        contactRestriction = QueryStore.getContactRestriction(),
                        segment = SegmentStore.sanitizeSegment({
                            'name': segmentData.name,
                            'display_name': segmentData.display_name,
                            'account_restriction': accountRestriction,
                            'contact_restriction': contactRestriction,
                            'page_filter': {
                                'row_offset': 0,
                                'num_rows': 10
                            }
                        });
                    // console.log('saveSegment existing', segmentData, segment)

                    SegmentService.CreateOrUpdateSegment(segment).then(function(result) {

                        vm.saveSegmentEnabled = false;
                        $state.go('.', { segment: 'segment' + ts }, { notify: false }, { reload: true });
                        vm.saved = true;

                    });
                });
            };
        };

    };

    vm.selectRatingEngineAttribute = function(enrichment) {
        DataCloudStore.selectRatingEngineAttribute(enrichment).then(function(response) {
            enrichment.IsRatingEngineAttribute = !enrichment.IsRatingEngineAttribute;
            //console.log(response);
        });
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
})
.directive('attributeFeedbackModal', function() {
    return {
        restrict: 'EA',
        templateUrl: '/components/datacloud/explorer/attributefeedback/attributefeedbackmodal.component.html',
        scope: {
            lookupResponse: '=?',
        },
        controller: ['$scope', '$document', '$timeout', '$window', 'LookupStore', 'DataCloudStore', 'BrowserStorageUtility', function ($scope, $document, $timeout, $window, LookupStore, DataCloudStore, BrowserStorageUtility) { 
            // test on LETest1503428538807_LPI
            var vm = $scope,
                lookupStore = LookupStore.get('request'),
                $modal = angular.element('attribute-feedback-modal');

            $scope.modal = DataCloudStore.getFeedbackModal();
            $scope.close = function() {
                DataCloudStore.setFeedbackModal(false);
            }

            var clientSession = BrowserStorageUtility.getClientSession();

            $scope.showLookupStore = $scope.modal.context.showLookupStore;
            if($scope.showLookupStore) {
                $scope.lookupStore = lookupStore;
            }

            $scope.icon = $scope.modal.context.icon;
            $scope.label = $scope.modal.context.label || $scope.modal.context.attribute.DisplayName;
            $scope.value = $scope.modal.context.value;
            $scope.categoryClass = $scope.modal.context.categoryClass;
            $scope.reported = false;

            $scope.report = function() {
                $scope.sendingReport = true;
                form_fields = $scope.input || {}
                report = {
                    Attribute: $scope.modal.context.attributeKey || $scope.modal.context.attribute.ColumnId || '',
                    MatchedValue: $scope.value,
                    CorrectValue: form_fields.value || '',
                    Comment: form_fields.comment || '',
                    InputKeys: lookupStore.record,
                    MatchedKeys: {
                        "LDC_Name": $scope.lookupResponse.attributes.LDC_Name,
                        "LDC_Domain": $scope.lookupResponse.attributes.LDC_Domain,
                        "LDC_City": $scope.lookupResponse.attributes.LDC_City,
                        "LDC_State": $scope.lookupResponse.attributes.LDC_State,
                        "LDC_ZipCode": $scope.lookupResponse.attributes.LDC_ZipCode,
                        "LDC_Country": $scope.lookupResponse.attributes.LDC_Country,
                        "LDC_DUNS": $scope.lookupResponse.attributes.LDC_DUNS
                    },
                    MatchLog: $scope.lookupResponse.matchLogs
                };

                $scope.sendingReport = false;
                $scope.reported = true;
                $timeout(function() {
                    $scope.close();
                }, 1000 * 5);
                DataCloudStore.sendFeedback(report, $scope.modal.context.type);
            }
            
            var setHeight = function() {
                var height = $(window).height() -20;
                $modal.css({maxHeight: height});
                $modal.find('.attribute-feedback-container').css({maxHeight: height});
            }

            $scope.setHeight = function() { // gets called by ng-inits so that the data is there and heights make sense
                setHeight();
            }

            var _handleDocumentResize = _.debounce(handleDocumentResize, 300);
            $(window).on('resize', _handleDocumentResize);

            function handleDocumentResize(evt) {
                setHeight();
            }

            $document.on('click', handleDocumentClick);

            function handleDocumentClick(evt) {
                var target = angular.element(evt.target),
                    clickedModal = target.parents('attribute-feedback-modal').length;

                if(!clickedModal) {
                    $scope.close();
                    $scope.$apply();
                }
            }

            $scope.$on('$destroy', function() {
                $document.off('click', handleDocumentClick);
                $(window).off('resize', _handleDocumentResize)
            });
        }]
    };
})
.directive('attributeFeedbackButton', function() {
    return {
        restrict: 'EA',
        templateUrl: '/components/datacloud/explorer/attributefeedback/attributefeedbackbutton.component.html',
        scope: {
            buttonType: '=?',
            attribute: '=',
            attributeKey: '=?',
            value: '=',
            iconImage: '=?',
            iconFont: '=?',
            categoryClass: '=?',
            label: '=?',
            showLookupStore: '=?',
            type: '=?'
        },
        controller: ['$scope', '$stateParams', 'DataCloudStore', function ($scope, $stateParams, DataCloudStore) {
            $scope.buttonType = $scope.buttonType || 'infodot';

            $scope.menuClick = function($event) {
                $event.stopPropagation();
                $scope.showMenu = !$scope.showMenu;
            }

            $scope.closeMenu = function($event) {
                $scope.showMenu = false;
            }
            $scope.open = function($event) {
                $event.stopPropagation();
                $scope.closeMenu($event);
                DataCloudStore.setFeedbackModal(true, {
                    attribute: $scope.attribute,
                    attributeKey: $scope.attributeKey,
                    value: $scope.value,
                    icon: {
                        image: $scope.iconImage || '',
                        font: $scope.iconFont || ''
                    },
                    categoryClass: $scope.categoryClass,
                    label: $scope.label || '',
                    showLookupStore: $scope.showLookupStore,
                    type: $scope.type
                });
            }
        }]
    };
});
