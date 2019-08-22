export default function (
    $scope, $filter, $timeout, $interval, $q, $state, $stateParams, $injector,
    LookupStore, Enrichments, BrowserStorageUtility, FeatureFlagService, DataCloudStore, DataCloudService,
    EnrichmentTopAttributes, EnrichmentPremiumSelectMaximum, EnrichmentSelectMaximum,
    QueryRestriction, WorkingBuckets, LookupResponse, QueryStore, ConfigureAttributesStore,
    RatingsEngineModels, RatingsEngineStore, QueryTreeService, ExplorerUtils, Notice
) {
    'ngInject';

    let ReviewData = $injector.has('ReviewData')
            ? $injector.get('ReviewData')
            : { success: false };

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
            deselected_messsage: 'Attribute will be turned off for enrichment',
            categories_see_all: 'See All Categories',
            categories_select_all: 'All Categories',
            premiumTotalSelectError: 'Premium attribute limit reached',
            generalTotalSelectError: 'Attribute limit reached',
            insufficientUserRights: 'Lattice Insights are disabled',
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
        EnrichmentSelectMaximum: EnrichmentSelectMaximum,
        lookupMode: (LookupResponse && LookupResponse.attributes !== null),
        lookupFiltered: LookupResponse.attributes,
        LookupResponse: LookupStore.response,
        no_lookup_results_message: false,
        hasCompanyInfo: (LookupStore.response && LookupStore.response.companyInfo ? Object.keys(LookupStore.response.companyInfo).length : 0),
        count: (LookupResponse.attributes ? Object.keys(LookupResponse.attributes).length : (Enrichments || []).length),
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
        cube: null,
        selected_categories: {},
        metadata: DataCloudStore.metadata,
        authToken: BrowserStorageUtility.getTokenDocument(),
        userSelectedCount: 0,
        selectDisabled: 1,
        saveDisabled: 1,
        selectedCount: 0,
        section: $stateParams.section,
        category: $stateParams.category,
        subcategory: $stateParams.subcategory,
        openHighlighter: {},
        categoryCounts: {},
        TileTableItems: {},
        workingBuckets: WorkingBuckets,
        pagesize: 24,
        categorySize: 7,
        addBucketTreeRoot: null,
        feedbackModal: DataCloudStore.getFeedbackModal(),
        stateParams: $stateParams,
        segment: $stateParams.segment,
        inWizard: false,
        datacollectionPrecheck: null,
        datacollectionPrechecking: false,
        collectionStatus: null
    });

    DataCloudStore.setMetadata('lookupMode', vm.lookupMode);

    function getDatacollectionPrecheck() {
        vm.datacollectionPrechecking = true; // spinner
        ConfigureAttributesStore.getPrecheck().then(function (result) {
            vm.datacollectionPrecheck = result;
            vm.datacollectionPrechecking = false;
        });
    }

    vm.init = function () {
        // leo();
        if (['segment.analysis'].indexOf(vm.section) != -1) { // only run on 'my data' page
            getDatacollectionPrecheck();
        }

        if ($state.current.name === 'home.ratingsengine.dashboard.segment.attributes.add' || $state.current.name === 'home.ratingsengine.rulesprospects.segment.attributes.add') {
            vm.mode = 'dashboardrules';
        }

        if (vm.section == 'wizard.ratingsengine_segment' && QueryStore.getAddBucketTreeRoot()) {
            //            vm.section = 'segment.analysis';
            vm.inWizard = true;
        }
        if ($state.current.name === 'home.ratingsengine.dashboard.segment.attributes.add') {
            vm.inWizard = false;
        }

        QueryStore.setSegmentEnabled = false;

        if (vm.section == 'insights' && !vm.show_lattice_insights) {
            vm.statusMessage(vm.label.insufficientUserRights, { wait: 0, special: 'nohtml' });
            return false;
        }

        if (vm.lookupMode && vm.LookupResponse.errorCode) {
            $state.go('home.datacloud.explorer');
        }


        // Only run in Atlas
        if (vm.section != 'insights' && vm.section != 'edit' && vm.section != 'team' && vm.section != 'lookup') {
            if (vm.section == 're.model_iteration') {
                var ratingId = $stateParams['rating_id'],
                    aiModel = $stateParams['aiModel'],
                    nocache = true,
                    opts = {
                        url: `/pls/ratingengines/${ratingId}/ratingmodels/${aiModel}/metadata/cube`
                    };
            } else {
                var nocache = true,
                    opts = {
                        url: `/pls/datacollection/statistics/cubes`
                    };
            }

            DataCloudStore.getCube(opts || {}, nocache || false).then(function (result) {
                vm.cube = result;
                if (vm.section == 're.model_iteration') {
                    vm.checkEnrichmentsForDisable(Enrichments);
                }
            });

        }

        vm.processCategories();
        vm.processEnrichments(Enrichments, true);
        vm.generateTree(true);

        if (vm.lookupMode && typeof vm.lookupFiltered === 'object' && (Object.keys(vm.lookupFiltered).length < 1 || LookupStore.hideLookupResponse(LookupResponse))) {
            vm.no_lookup_results_message = true;
        }

        if (vm.section === 'segment.analysis') {
            // vm.setCurrentRestrictionForSaveButton();
            vm.metadataSegments = QueryRestriction;
        }

        // for Advanced Query Builder
        vm.addBucketTreeRoot = QueryStore.getAddBucketTreeRoot();

        $scope.$on("$destroy", function () {
            delete (vm.addBucketTreeRoot = QueryStore.setAddBucketTreeRoot(null));
        });

        DataCloudStore.setFeedbackModal(false);

        if (vm.section == 'segment.analysis') {
            QueryStore.getCollectionStatus().then(function (result) {
                vm.collectionStatus = result;
            });
        }

        DataCloudStore.setMetadata('current', 1);

    }

    vm.checkEnrichmentsForDisable = function(enrichments) {
        enrichments.forEach(function (enrichment) {
            if (vm.cube && vm.cube.data && vm.cube.data[enrichment.Entity] && vm.cube.data[enrichment.Entity].Stats && vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId]) {
                if (
                    vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts == undefined || 
                    vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts.List == undefined || 
                    !vm.cube.data[enrichment.Entity].Stats[enrichment.ColumnId].Bkts.List.length
                ) {
                    enrichment.ApprovedUsage[0] = 'None';
                }
            }
        });
    }

    /* some rules that might hide the page */
    vm.hidePage = function () {
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

    vm.relanding = function () {
        vm.TileTableItems = {};

        if (vm.metadataSegments || QueryRestriction) {
            getExplorerSegments(vm.enrichments);
        }
    }

    vm.filter = function (items, property, value, debug) {
        if (property.indexOf('.') > -1) {
            var propsList = property.split('.');
            var walkObject = function (obj, j) {
                if (obj && j) {
                    return obj[j];
                }
            };

            for (var i = 0, result = [], item; i < items.length; i++) {
                item = propsList.reduce(walkObject, items[i]);

                if (typeof item != 'undefined' && item == value) {
                    result.push(items[i]);
                }
            }
        } else {
            var result = items.filter(function (item) {
                return item[property] == value;
            });
        }

        if (debug) {
            // console.log('prop:', property, 'value:', value, 'items:', items, 'result:', result);
        }

        return result;
    }

    vm.closeHighlighterButtons = function (index) {
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

    vm.setSubcategory = function (subcategory) {
        vm.subcategory = subcategory;
        DataCloudStore.setMetadata('subcategory', subcategory);
    }

    vm.getAttributes = function (category) {
        var attributes = vm.enrichmentsObj[category];
        // console.log('*******************************************');
        // console.log(attributes);
        // console.log('*******************************************');

        var subcategory = DataCloudStore.getMetadata('subcategory');
        var ret = [];
        for (var i = 0; i < attributes.length; i++) {
            if (attributes[i].Subcategory === subcategory) {
                ret.push(attributes[i]);
            }
        }

        // console.log(ret.find( enrichment => enrichment.DisplayName === 'Domestic HQ Country') );
        // console.log(ret.find( enrichment => enrichment.DisplayName === 'Latest Funding Date') );

        return ret;
    }

    vm.subcategoryRenamer = function (string, replacement) {
        if (string) {
            var replacement = replacement || '';

            return string.toLowerCase().replace(/\W+/g, replacement);
        }

        return '';
    }

    vm.checkEmptyCategory = function (category, count, isEnabled) {
        if (vm.category && !vm.categoryCount(vm.category) && count && isEnabled) {
            vm.setCategory(category);
        }
        return false;
    }

    vm.updateStateParams = function () {
        $state.go('.', {
            category: vm.category,
            subcategory: vm.subcategory
        });
    }

    var stopNumbersInterval = function () {
        if (numbersInterval) {
            $interval.cancel(numbersInterval);
            vm.placeholderTotal = null;
        }
    }

    vm.processEnrichments = function (enrichments) {
        if (vm.lookupFiltered !== null) {
            for (var i = 0, _enrichments = []; i < enrichments.length; i++) {
                if (vm.lookupFiltered && vm.lookupFiltered[enrichments[i].ColumnId]) {
                    _enrichments.push(enrichments[i]);
                }
            }
        } else {
            var _enrichments = enrichments;
        }

        for (var i = 0, enrichment; i < _enrichments.length; i++) {
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

            vm.enrichmentsMap[enrichment.Entity + '.' + enrichment.ColumnId] = vm.enrichments.length;
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

    vm.generateTree = function (isComplete) {
        var timestamp = new Date().getTime();
        var obj = {};

        vm.enrichments.forEach(function (item, index) {
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

            if (vm.section == 'wizard.ratingsengine_segment') {
                item.IsRatingsEngineAttribute = ratingsEngineAttributeState(item);
            }

            obj[category][subcategory].push(index);
        });

        var timestamp2 = new Date().getTime();
        // console.info('generateTree();\t\t\t', (timestamp2 - timestamp) + 'ms');

        if (isComplete) {
            vm.categories.forEach(function (category, item) {
                if (obj[category]) {
                    getEnrichmentSubcategories(category, Object.keys(obj[category]));
                }
            });

            if (vm.section === 'wizard.ratingsengine_segment' || vm.section === 'dashboard.rules') {
                var SelectedForRatingsEngine = vm.filter(vm.enrichments, 'IsRatingsEngineAttribute', true);
                DataCloudStore.setMetadata('selectedForRatingsEngine', SelectedForRatingsEngine.length);
                RatingsEngineStore.setValidation('attributes', (SelectedForRatingsEngine.length > 0));
            }

            getTopAttributes();
            getHighlightMetadata();

            if (vm.metadataSegments || QueryRestriction) {
                getExplorerSegments(vm.enrichments);
            }
        }
    }

    var getEnrichmentSubcategories = function (category, subcategories) {
        vm._subcategories[category] = subcategories;
        vm.subcategories[category] = subcategories;

        if (subcategories.length <= 1) {
            vm.subcategoriesExclude.push(category);
        }

        vm.filterEmptySubcategories();
    }

    vm.removeFromArray = function (array, item) {
        if (array && item) {
            var index = array.indexOf(item);
            if (index > -1) {
                array.splice(index, 1);
            }
        }
    }

    vm.filterEmptySubcategories = function () {
        if (vm._subcategories[vm.category]) {
            for (var i = 0, newCategories = []; i < vm._subcategories[vm.category].length; i++) {
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

    var addUniqueToArray = function (array, item) {
        if (array && item && !array.indexOf(item)) {
            array.push(item);
        }
    }

    var breakOnFirstEncounter = function (items, property, value, returnObj) {
        for (var i = 0, item; i < items.length; i++) {
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

    var getHighlightMetadata = function () {
        var timestamp = new Date().getTime();
        //console.log(vm.categories);
        vm.categories.forEach(function (category) {
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

                    vm.subcategories[category].forEach(function (subcategory) {
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

    vm.showHighlighting = function () {
        return (vm.section == 'team' || vm.section == 'insights' || vm.section == 'lookup');
    }

    vm.highlightTypesCategoryLabel = function (category, subcategory) {
        var category = category || '',
            subcategory = subcategory || '',
            metadata = {},
            type = 'enabled',
            label = vm.highlightTypesCategory[type];

        if (category) {
            metadata = vm.highlightMetadata.categories[category];
            if (subcategory && vm.highlightMetadata.categories[category].subcategories) {
                metadata = vm.highlightMetadata.categories[category].subcategories[subcategory];
            }
        }

        if (metadata) {
            if (metadata.enabled && metadata.disabled) {
                type = 'dirty';
                label = 'Some Enabled for Sales Team';
            } else if (!metadata.enabled) {
                type = 'disabled';
                label = vm.highlightTypesCategory[type];
            }
        }

        return {
            type: type,
            label: label
        };
    }

    var highlightOptionsInitState = function (enrichment) {
        var ret = { type: 'enabled', label: '', highlighted: false, enabled: false };

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

    var getRatingsEngineRule = function (RatingsEngineModels) {
        var data = RatingsEngineModels,
            rule = (data && data.rule ? data.rule : {}),
            rule = rule || {};
        return rule;
    }

    var ratingsEngineAttributeState = function (item) {
        var rule = getRatingsEngineRule(RatingsEngineModels),
            ratingsEngineAttributes = rule.selectedAttributes || [];

        if (ratingsEngineAttributes.indexOf(item.Entity + '.' + item.ColumnId) >= 0) {
            return true;
        }

        return false;
    }

    vm.getArray = function (number) {
        return new Array(number);
    }

    var getFlags = function (opts) {
        var deferred = $q.defer();

        DataCloudStore.getFlags(opts).then(function (result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    }

    var setFlags = function (opts, flags) {
        var deferred = $q.defer();

        DataCloudService.setFlags(opts, flags).then(function (result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    }

    vm.setFlags = function (type, enrichment) {
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
        vm.statusMessage(vm.label.saving_alert, { wait: 0 });

        setFlags(opts, flags).then(function () {
            vm.statusMessage(vm.label.saved_alert, { type: 'saved' });
            vm.closeHighlighterButtons();

            enrichment.HighlightState = { type: type, label: label, enabled: !flags.hidden, highlighted: flags.highlighted };
            enrichment.HighlightHidden = flags.hidden;
            enrichment.HighlightHighlighted = flags.highlighted;

            vm.enrichments.find(function (i) { return i.ColumnId === enrichment.ColumnId; }).AttributeFlagsMap = {
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

    var setFlagsByCategory = function (opts, flags) {
        var deferred = $q.defer();

        if (opts.subcategoryName) {
            DataCloudService.setFlagsBySubcategory(opts, flags).then(function (result) {
                deferred.resolve(result);
            });
        } else if (opts.categoryName) {
            DataCloudService.setFlagsByCategory(opts, flags).then(function (result) {
                deferred.resolve(result);
            });
        }

        return deferred.promise;
    }

    vm.setFlagsByCategory = function (type, category, subcategory) {
        var opts = {
            categoryName: category,
            subcategoryName: subcategory
        },
            flags = {
                hidden: (type === 'enabled' ? false : true),
            },
            settings_changed = vm.highlightTypesCategoryLabel(category, subcategory).type !== type;

        if (!settings_changed) {
            return false;
        }

        if (!vm.highlightMetadata.categories[category][type]) {
            var changed = true;
        }

        if (type === 'disabled') {
            flags.highlighted = false;
        }


        vm.statusMessage(vm.label.saving_alert, { wait: 0 });

        setFlagsByCategory(opts, flags).then(function () {
            vm.statusMessage(vm.label.saved_alert, { type: 'saved' });
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

                if (!enrichment.AttributeFlagsMap) {
                    enrichment.AttributeFlagsMap = {};
                }
                if (!enrichment.AttributeFlagsMap.CompanyProfile) {
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

    vm.filterLookupFiltered = function (item, type) {
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

    var getTopAttributes = function (opts) {
        var opts = opts || {},
            category = opts.category;

        DataCloudStore.getAllTopAttributes().then(function (result) {
            var timestamp = new Date().getTime();

            Object.keys(EnrichmentTopAttributes).forEach(function (catKey, catItem) {
                var category = EnrichmentTopAttributes[catKey]['Subcategories'];

                Object.keys(category).forEach(function (subcategory) {
                    var items = category[subcategory];

                    items.forEach(function (item) {

                        var enrichment = vm.enrichments[vm.enrichmentsMap[item.Entity + '.' + item.Attribute]];

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

    vm.removeNullsFromArray = function (array) {
        var ret = [];
        array.forEach(function (item, index) {
            if (item) {
                ret.push(item);
            }
        });
        return ret;
    }

    vm.removeEmptyCategories = function (categories) {
        var ret = [];
        var loopItemNum = 0;

        categories.forEach(function (category, index) {
            if (vm.categoryCount(category)) {
                if (loopItemNum == 0) {
                    DataCloudStore.setMetadata('subheadercategory', category);
                }
                ret.push(category);
                loopItemNum++;
            }
        });
        return ret;
    }

    vm.processCategories = function () {
        // console.log(EnrichmentTopAttributes);

        vm.categories = Object.keys(EnrichmentTopAttributes).sort();
        if ((vm.show_segmentation && vm.section == 'segment.analysis') || vm.section == 'wizard.ratingsengine_segment' || vm.section == 'dashboard.rules') {
            DataCloudStore.topCategories.forEach(function (category, index) {
                if (vm.categories.indexOf(category) >= 0) {
                    swap(vm.categories, vm.categories.indexOf(category), index);
                }
            });
        }

        DataCloudStore.setCategories(vm.categories);

        for (var i in vm.categories) {
            vm.categoryCounts[vm.categories[i]] = null;
        }
    }

    vm.getTileTableItems = function (category, subcategory, segment, limit, debug) {
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

            if (!subcategory) { //PLS-4922
                Object.keys(vm.topAttributes[category].Subcategories).forEach(function (key, index) {
                    items = items.concat(vm.topAttributes[category].Subcategories[key]);
                });
            } else {
                items = vm.topAttributes[category].Subcategories[subcategory];
            }

            var timestamp_b = new Date().getTime();

            if (items) {
                items.forEach(function (item, itemKey) {
                    var index = vm.enrichmentsMap[item.Entity + '.' + item.Attribute],
                        enrichment = vm.enrichments[index],
                        map = [
                            //'Value',
                            //'AttributeValue',
                            'FundamentalType',
                            'DisplayName',
                            'Category',
                            'Subcategory',
                            'IsSelected',
                            'IsPremium',
                            'IsInternal',
                            'ImportanceOrdering',
                            'HighlightHidden',
                            'HighlightHighlighted',
                            'SegmentChecked',
                            'IsRatingsEngineAttribute'
                        ];

                    if (enrichment) {
                        if (!vm.lookupMode) {
                            map.forEach(function (key) {
                                item[key] = enrichment[key];
                            });

                            enrichment.Count = item.Count;
                        }
                        item.Hide = false;
                        if (!vm.searchFields(enrichment)) {
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
                items = items.filter(function (item) {
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

        if (segment === 'HighlightHighlighted' && !vm.showHighlighting()) {
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

    vm.filterHideTrue = function (item) {
        if (item.Hide === true) {
            return false;
        } else {
            return true;
        }

    }

    vm.generateTileTableLabel = function (items) {
        if (items) {
            if (vm.section == 'segment.analysis') {
                return 'Attribute';
            }

            return 'Top ' + (items.length > 1 ? items.length + ' attributes' : 'attribute');
        }

        return '';
    }

    vm.inCategory = function (enrichment) {
        if (enrichment.DisplayName && !(_.size(vm.selected_categories))) { // for case where this is used as a | filter in the enrichments ngRepeat on initial state
            return true;
        }

        var selected = (typeof vm.selected_categories[enrichment.Category] === 'object');

        return selected;
    };

    vm.selectEnrichment = function (enrichment) {
        vm.saveDisabled = 0;
        vm.selectDisabled = 0;
        var selectedTotal = vm.filter(vm.enrichments, 'IsSelected', true);

        if (enrichment.IsPremium) {
            vm.premiumSelectedTotal = vm.filter(selectedTotal, 'IsPremium', true).length;
            if (vm.premiumSelectedTotal > vm.premiumSelectLimit && enrichment.IsSelected) {
                vm.premiumSelectedTotal = vm.premiumSelectLimit;
                enrichment.IsSelected = false;
                enrichment.IsDirty = false;
                vm.statusMessage(vm.label.premiumTotalSelectError);
                return false;
            }
        }

        vm.generalSelectedTotal = selectedTotal.length;

        if (vm.generalSelectedTotal > vm.generalSelectLimit && enrichment.IsSelected) {
            vm.generalSelectedTotal = vm.generalSelectLimit;
            enrichment.IsSelected = false;
            enrichment.IsDirty = false;
            vm.statusMessage(vm.label.generalTotalSelectError);
            return false;
        }

        if (enrichment.IsSelected) {
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
                vm.statusMessage(vm.label.disabled_alert, { type: 'disabling', wait: 0 });
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
    vm.statusMessage = function (message, opts, callback) {
        var opts = opts || {},
            wait = (opts.wait || opts.wait === 0 ? opts.wait : 1500),
            type = opts.type || 'alert';

        vm.status_alert.type = type;
        vm.status_alert.message = message;
        vm.status_alert.special = opts.special;
        $timeout.cancel(status_timer);
        vm.status_alert.show = true;

        if (wait) {
            status_timer = $timeout(function () {
                vm.status_alert.show = false;
                vm.status_alert.message = '';
                if (typeof callback === 'function') {
                    callback();
                }
            }, wait);
        }
    }

    vm.closeStatusMessage = function () {
        $timeout.cancel(status_timer);
        vm.status_alert.show = false;
        vm.status_alert.message = '';
    }

    vm.saveSelected = function () {
        var dirtyEnrichments = vm.filter(vm.enrichments, 'IsDirty', true),
            selectedObj = vm.filter(dirtyEnrichments, 'IsSelected', true),
            deselectedObj = vm.filter(dirtyEnrichments, 'IsSelected', false),
            selected = [],
            deselected = [];

        // console.log('saveSelected', dirtyEnrichments, selectedObj, deselectedObj);
        vm.selectDisabled = (selectedObj.length ? 0 : 1);

        for (var i in selectedObj) {
            if (selectedObj[i].ColumnId) {
                selected.push(selectedObj[i].ColumnId);
            }
        }

        for (var i in deselectedObj) {
            if (deselectedObj[i].ColumnId) {
                deselected.push(deselectedObj[i].ColumnId);
            }
        }

        var data = {
            selectedAttributes: selected,
            deselectedAttributes: deselected
        }

        vm.saveDisabled = 1;
        vm.hasSaved = 0;

        vm.statusMessage(vm.label.saving_alert, { wait: 0 });

        DataCloudService.setEnrichments(data).then(function (result) {
            if (result.errorCode) {
                vm.statusMessage(vm.label.saved_error, { type: 'error' });
            } else {
                vm.statusMessage(vm.label.saved_alert, { type: 'saved' });
            }
            vm.saveDisabled = 1;

            if (selectedObj.length > 0 || deselectedObj.length > 0) {
                var dirtyObj = vm.filter(vm.enrichments, 'IsDirty', true);

                for (var i in dirtyObj) {
                    dirtyObj[i].IsDirty = false;
                }
            }
        });
    }

    vm.fieldType = function (fieldType) {
        var fieldType = fieldType.replace(/[0-9]+/g, '*');
        var fieldTypes = {
            'default': 'Text/String',
            'NVARCHAR(*)': 'Text/String',
            'FLOAT': 'Number/Float',
            'INT': 'Number/Int',
            'BOOLEAN': 'Boolean'
        };

        return fieldTypes[fieldType] || fieldTypes.default;
    }

    /* jumps you to non-empty category when you filter */
    var gotoNonemptyCategory = function () {
        var treeRoot = QueryStore.getAddBucketTreeRoot();

        if (treeRoot) {
            return false;
        }

        var categories = [],
            category = '';

        if (vm.categoryCounts) {
            for (var i in vm.categoryCounts) {
                if (vm.categoryCounts[i] > 0) {
                    categories.push(i);
                }
            }
        }

        if (categories.length <= 1 && !vm.lookupMode) {
            vm.setCategory(categories[0]);
        }
    }

    vm.setCategory = function (category) {
        vm.category = category;
        DataCloudStore.setMetadata('category', category);
    }

    vm.isYesNoCategory = function (category, includeKeywords) {
        var list = ['Website Profile', 'Technology Profile', 'Product Spend Profile'];

        if (includeKeywords) {
            list.push('Website Keywords');
            list.push('Product');
        }

        return list.indexOf(category) >= 0;
    }

    vm.hasSubcategories = function (category) {
        return vm._subcategories[category].length != 1 || vm._subcategories[category][0] != 'Other';
    }

    vm.checkSelectedRatingEngineAttrs = function () {
        return (typeof vm.metadata.toggle.show.selected_ratingsengine_attributes === 'undefined') ? false : vm.metadata.toggle.show.selected_ratingsengine_attributes;
    }

    vm.percentage = function (number, total, suffix, limit) {
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

    var ObjectValues = function (obj) {
        var ar = [];
        if (obj && typeof obj === 'object') {
            for (var i in obj) {
                ar.push(obj[i]);
            }
        }
        return ar;
    }

    vm.getHighestLift = function (stats) {
        if (vm.section == 're.model_iteration') {
            var highest = 0;
            stats.forEach(function (stat) {
                if (stat.Lift.toFixed(1) > highest) {
                    highest = stat.Lift.toFixed(1);
                }
            })
            return highest;
        } else {
            return '';
        }
    }

    vm.getTotalBucketCounts = function (stats) {
        var bucketsTotal = stats.reduce(function(prev, cur) {
          return prev + cur.Cnt;
        }, 0);
        return bucketsTotal;
    }

    vm.getHighestStat = function (stats) {
        var highest = 0;
        stats.forEach(function (stat) {
            if (stat.Cnt > highest) {
                highest = stat.Cnt;
            }
        })
        return highest;
    }

    vm.makeSegmentsRangeKey = function (enrichment, range, label) {
        var fieldName = enrichment.Attribute || enrichment.ColumnId,
            values = ObjectValues(range),
            key = fieldName + (range ? values.join('') : label);

        return key;
    }

    var getBucketRestrictions = function (tree, arr) {
        tree.forEach(function (branch) {
            if (branch && branch.bucketRestriction) {
                arr.push(branch);
            }

            if (branch && branch.logicalRestriction) {
                getBucketRestrictions(branch.logicalRestriction.restrictions, arr);
            }
        });
    }

    var getExplorerSegments = function (enrichments) {

        if (vm.metadataSegment != undefined) {
            var accountRestrictions = vm.metadataSegments.accountRestrictions,
                contactRestrictions = vm.metadataSegments.contactRestrictions;
        } else {
            var queryRestriction = QueryRestriction,
                accountRestrictions = queryRestriction.accountRestrictions,
                contactRestrictions = queryRestriction.contactRestrictions;
        }

        if (vm.addBucketTreeRoot) {
            return;
        }

        accountRestrictions = accountRestrictions
            ? angular.copy(accountRestrictions.restriction.logicalRestriction.restrictions)
            : [];

        contactRestrictions = contactRestrictions
            ? angular.copy(contactRestrictions.restriction.logicalRestriction.restrictions)
            : [];

        var restrictions = [];

        // FIXME:  Recursively grab all bucketRestrictions - but we might not want this
        // getBucketRestrictions(accountRestrictions, restrictions);
        // getBucketRestrictions(contactRestrictions, restrictions);

        restrictions = [].concat(accountRestrictions, contactRestrictions);

        //console.log('getExplorerSegments', accountRestrictions, contactRestrictions, restrictions);

        // FIXME: this should be recursive... -Lazarus
        for (var i = 0; i < restrictions.length; i++) {
            var restriction = restrictions[i];

            if (!restriction || !restriction.bucketRestriction || !restriction.bucketRestriction.bkt) {
                continue;
            }

            var restriction = restriction.bucketRestriction,
                range = restriction.bkt.Rng,
                label = restriction.bkt.Lbl,
                key = restriction.attr.split(".")[1],
                enrichment = breakOnFirstEncounter(vm.enrichments, 'ColumnId', key, true),
                index = vm.enrichmentsMap[restriction.attr];

            if (index || index === 0) {
                vm.enrichments[index].SegmentChecked = true;
                vm.enrichments[index].SegmentRangesChecked = {};
                vm.segmentAttributeInput[vm.enrichments[index].ColumnId] = true;
                vm.segmentAttributeInputRange[vm.makeSegmentsRangeKey(enrichment, range, label)] = true;
            }
        }
    }

    var textSearch = function (haystack, needle, case_insensitive) {
        var case_insensitive = (case_insensitive === false ? false : true);

        if (case_insensitive) {
            var haystack = haystack.toLowerCase(),
                needle = needle.toLowerCase();
        }

        // .indexOf is faster and more supported than .includes
        return (haystack.indexOf(needle) >= 0);
    }

    vm.searchFields = function (enrichment) {
        if (vm.query) {
            if (enrichment.DisplayName && textSearch(enrichment.DisplayName, vm.query)) {
                return true;
            } else if (enrichment.Description && textSearch(enrichment.Description, vm.query)) {
                return true;
            } else if (enrichment.Subcategory && textSearch(enrichment.Subcategory, vm.query)) {
                return true;
            } else {
                return false;
            }
        }

        return true;
    }

     vm.subcategoryCount = function (category, subcategory) {
            var filtered = vm.enrichmentsObj[category];

            vm.hasCategoryCount = 0;

            if (!filtered || filtered.length <= 0) {
                return 0;
            }

            for (var i = 0, result = []; i < filtered.length; i++) {
                var item = filtered[i];
                if (item && vm.searchFields(item)) {
                    if (
                        item.Category != category ||
                        item.Subcategory != subcategory ||
                        (vm.lookupMode &&
                            !vm.metadata.toggle.show.nulls &&
                            item.AttributeValue == 'No' &&
                            vm.isYesNoCategory(category)) ||
                        (vm.metadata.toggle.show.selected &&
                            !item.IsSelected) ||
                        (vm.metadata.toggle.hide.selected && item.IsSelected) ||
                        (vm.metadata.toggle.show.premium && !item.IsPremium) ||
                        (vm.metadata.toggle.hide.premium && item.IsPremium) ||
                        (!vm.metadata.toggle.show.internal &&
                            item.IsInternal) ||
                        (vm.metadata.toggle.show.enabled &&
                            item.HighlightHidden) ||
                        (vm.metadata.toggle.hide.enabled &&
                            !item.HighlightHidden) ||
                        (vm.metadata.toggle.show.highlighted &&
                            !item.HighlightHighlighted) ||
                        (vm.metadata.toggle.hide.highlighted &&
                            item.HighlightHighlighted) ||
                        (vm.metadata.toggle.show
                            .selected_ratingsengine_attributes &&
                            !item.IsRatingsEngineAttribute)
                    ) {
                        continue;
                    }
                    result.push(item);
                }
            }

            return result.length;
        };
    

    vm.categoryCount = function (category) {
            var filtered = vm.enrichmentsObj[category],
                iterationFilter = DataCloudStore.getRatingIterationFilter();

            if (!filtered) {
                return 0;
            }

            for (var i = 0, result = []; i < filtered.length; i++) {
                var item = filtered[i];

                if (item && vm.searchFields(item)) {
                    if (
                        item.Category != category ||
                        (vm.lookupMode &&
                            !vm.metadata.toggle.show.nulls &&
                            item.AttributeValue == 'No' &&
                            vm.isYesNoCategory(category)) ||
                        (vm.metadata.toggle.show.selected &&
                            !item.IsSelected) ||
                        (vm.metadata.toggle.hide.selected && item.IsSelected) ||
                        (vm.metadata.toggle.show.premium && !item.IsPremium) ||
                        (vm.metadata.toggle.hide.premium && item.IsPremium) ||
                        (!vm.metadata.toggle.show.internal &&
                            item.IsInternal) ||
                        (vm.metadata.toggle.show.enabled &&
                            item.HighlightHidden) ||
                        (vm.metadata.toggle.hide.enabled &&
                            !item.HighlightHidden) ||
                        (vm.metadata.toggle.show.highlighted &&
                            !item.HighlightHighlighted) ||
                        (vm.metadata.toggle.hide.highlighted &&
                            item.HighlightHighlighted) ||
                        (vm.metadata.toggle.show
                            .selected_ratingsengine_attributes &&
                            !item.IsRatingsEngineAttribute) ||
                        (iterationFilter == 'used' &&
                            !('ImportanceOrdering' in item)) ||
                        (iterationFilter == 'warnings' &&
                            !item.HasWarnings) ||
                        (iterationFilter == 'disabled' &&
                            item.ApprovedUsage[0] != 'None')
                    ) {
                        continue;
                    }
                    result.push(item);
                }
            }
            vm.categoryCounts[item.Category] = result.length;

            if ($stateParams.gotoNonemptyCategory) {
                gotoNonemptyCategory();
            }

            vm.hasCategoryCount = result.length;
            if (vm.lookupMode) {
                lookupSyncNewTotal();
            }
            return result.length;
        };

    var lookupSyncNewTotal = function () {

        var categories = vm.highlightMetadata.categories,
            highlightedCategories = [],
            total = 0;

        for (var key in categories) {
            if (categories[key].enabled === 1) {
                highlightedCategories.push(key);
            }
        };

        highlightedCategories.forEach(function (category) {
            for (var key in vm.categoryCounts) {
                if (key === category) {
                    total += vm.categoryCounts[key];
                }
            };
        });

        LookupStore.add('count', total);
    }


    var getSegmentBucketInputs = function () {
        var buckets = {},
            metadataSegments = vm.metadataSegments || QueryRestriction;
        if (metadataSegments && metadataSegments.all) {
            metadataSegments.all.forEach(function (item) {
                var bucketId = item.bucketRestriction.lhs.columnLookup.column_name + item.bucketRestriction.range.max;

                buckets[bucketId] = true;
            });
        }
        return buckets;
    }


    vm.segmentAttributeInput = DataCloudStore.getMetadata('segmentAttributeInput') || {};
    vm.selectSegmentAttribute = function (attribute) {
        if (!vm.cube) {
            Notice.warning({
                delay: 1500,
                message: 'Your data is still loading. One moment please.'
            });
        }

        if (!attribute.TopBkt && !vm.inWizard) {
            vm.addFreeTextAttribute(attribute, vm.cube.data[attribute.Entity].Stats[attribute.Attribute]);
            return;
        }

        attribute.SegmentChecked = true;

        var attribute = angular.copy(attribute),
            attributeKey = attribute.Attribute || attribute.FieldName,
            stat = vm.getAttributeStat(attribute) || {},
            attributeRangeKey = (stat.Lbl ? vm.makeSegmentsRangeKey(attribute, stat.Rng, stat.Lbl) : ''),

            index = vm.enrichmentsMap[attribute.Entity + '.' + attributeKey],
            enrichment = vm.enrichments[index],

            entity = attribute.Entity,
            topBkt = attribute.TopBkt,
            segmentName = $stateParams.segment;

        vm.segmentAttributeInput[attributeKey] = !vm.segmentAttributeInput[attributeKey];
        DataCloudStore.setMetadata('segmentAttributeInput', vm.segmentAttributeInput);

        if (attributeRangeKey) {
            vm.segmentAttributeInputRange[attributeRangeKey] = !vm.segmentAttributeInputRange[attributeRangeKey];
        }

        var addEntity = entity == 'Rating' || entity == 'LatticeAccount' || entity == 'CuratedAccount' ? 'Account' : entity;

        var attributeData = {
            columnName: attributeKey,
            resourceType: entity
        };

        if (!vm.inWizard) {
            attributeData.bkt = angular.copy(topBkt);
        }

        QueryStore.counts.accounts.loading = true;
        QueryStore.counts.contacts.loading = true;
        QueryStore['add' + addEntity + 'Restriction'](attributeData);

        vm.checkSaveButtonState();
    }

    vm.segmentAttributeInputRange = vm.segmentAttributeInputRange || {};
    vm.selectSegmentAttributeRange = function (enrichment, stat, disable) {
        var disable = disable || false,
            attributeKey = enrichment.Attribute || enrichment.ColumnId || enrichment.ColumnId,
            attributeRangeKey = vm.makeSegmentsRangeKey(enrichment, stat.Rng, stat.Lbl),
            fieldName = enrichment.ColumnId || enrichment.ColumnId,
            entity = enrichment.Entity,
            segmentName = $stateParams.segment;

        enrichment.SegmentChecked = true;

        if (disable) {
            return false;
        }
        var bucketToAdd = entity;
        if (entity === 'LatticeAccount' || entity === 'Transaction' || entity === 'Rating' || entity === 'CuratedAccount') {
            bucketToAdd = 'Account';
        }

        vm.segmentAttributeInput[attributeKey] = !vm.segmentAttributeInput[attributeKey];
        vm.segmentAttributeInputRange[attributeRangeKey] = !vm.segmentAttributeInputRange[attributeRangeKey];

        QueryStore.counts.accounts.loading = true;
        QueryStore.counts.contacts.loading = true;
        QueryStore['add' + bucketToAdd + 'Restriction']({
            columnName: attributeKey,
            resourceType: entity,
            bkt: angular.copy(stat)
        });

        vm.TileTableItems = {};

        if (vm.metadataSegments || QueryRestriction) {
            getExplorerSegments(vm.enrichments);
        }

        vm.checkSaveButtonState();

    }

    vm.addFreeTextAttribute = function (enrichment, cube) {
        var count = cube ? cube.Cnt : 0;
        var bkt = { //default bucket for free-text attributes added in My Data or Add step of rules-based rating engine
            'Cmp': 'IS_NOT_NULL',
            'Cnt': count,
            'Id': -1,
            'Lbl': '*',
            'Vals': [
                ''
            ]
        };
        // vm.selectSegmentAttributeRange(enrichment, bkt, (vm.section != 'segment.analysis'));
        vm.selectSegmentAttributeRange(enrichment, bkt, false);
    }

    var getSegmentBucketInputs = function () {
        var buckets = {},
            metadataSegments = vm.metadataSegments || QueryRestriction;
        if (metadataSegments && metadataSegments.all) {
            metadataSegments.all.forEach(function (item) {
                var bucketId = item.bucketRestriction.lhs.columnLookup.column_name + item.bucketRestriction.range.max;
                buckets[bucketId] = true;
            });
        }
        return buckets;
    }

    vm.segmentBucketInput = getSegmentBucketInputs();
    vm.selectBucketInput = function (id, bucket) {
        // console.log('Explorer ', id, ' === ',bucket);
        var bucketId = id + bucket,
            range = { min: bucket, max: bucket, is_null_only: false };

        vm.segmentBucketInput[bucketId] = !vm.segmentBucketInput[bucketId];

        QueryStore.setPublicProperty('enableSaveSegmentButton', true);
        //QueryStore.saveSegmentEnabled = true;
        if (vm.segmentBucketInput[bucketId] === true) {
            QueryStore.addAccountRestriction({ columnName: id, range: range });
        } else {
            QueryStore.removeAccountRestriction({ columnName: id, range: range });
        }

    }

    vm.inModel = function () {
        var name = $state.current.name.split('.');
        return name[1] == 'model';
    }

    vm.refineQuery = function () {
        if (vm.inModel()) {
            $state.go('home.model.analysis.explorer.builder');
        } else {
            $state.go('home.segment.explorer.builder');
        }
    }

    vm.checkSaveButtonState = function () {
        var oldVal = QueryStore.getDefaultRestrictions(),
            newAccountVal = JSON.stringify(QueryStore.getAccountRestriction()),
            newContactVal = JSON.stringify(QueryStore.getContactRestriction()),
            newVal = newAccountVal + newContactVal;

        QueryStore.setPublicProperty('enableSaveSegmentButton', (oldVal !== newVal));
    };


    vm.selectRatingsEngineAttribute = function (enrichment) {
        var treeRoot = QueryStore.getAddBucketTreeRoot();

        if (treeRoot) {

            var enrichments = vm.topAttributes[enrichment.Category].Subcategories[enrichment.Subcategory],
                attribute = enrichments.filter(function (item) { return item.Attribute == enrichment.ColumnId })[0];

            vm.selectSegmentAttribute(attribute);

            RatingsEngineStore.setValidation('add', true);
            vm.statusMessage(vm.label.saved_alert, { type: 'saved' });
        } else {

            vm.statusMessage(vm.label.saving_alert, { wait: 0 });

            var rule = getRatingsEngineRule(RatingsEngineModels);
            var entity = enrichment.Entity;
            var attr = enrichment.ColumnId;
            DataCloudStore.selectRatingsEngineAttributes($stateParams.rating_id, rule.id, [enrichment.Entity + '.' + enrichment.ColumnId]).then(function (response) {
                ExplorerUtils.removeAddAttrFromRule(!enrichment.IsRatingsEngineAttribute, rule, entity, attr);
                enrichment.IsRatingsEngineAttribute = !enrichment.IsRatingsEngineAttribute;

                var SelectedForRatingsEngine = vm.filter(vm.enrichments, 'IsRatingsEngineAttribute', true);

                DataCloudStore.setMetadata('selectedForRatingsEngine', SelectedForRatingsEngine.length);

                if (!SelectedForRatingsEngine.length) {
                    vm.metadata.toggle.show.selected_ratingsengine_attributes = false;
                }

                RatingsEngineStore.setValidation('attributes', true);
                vm.statusMessage(vm.label.saved_alert, { type: 'saved' });
            });
        }
    }

    vm.getAttributeRules = function (attribute, bucket) {
        var getEmptyBuckets = vm.mode == 'dashboardrules';
        var attributes = QueryStore.getDataCloudAttributes(true, getEmptyBuckets); // second parm is getEmptyBuckets

        attributes = attributes.filter(function (item) {
            var restriction = item.bucketRestriction,
                isSameAttribute = restriction.attr == attribute.Entity + '.' + (attribute.Attribute || attribute.ColumnId),
                isSameBucket = true,
                bkt = restriction.bkt,
                ret = isSameAttribute;

            if (bucket && bkt) {
                ret = QueryTreeService.getAttributeRules(restriction, bkt, bucket, isSameAttribute);
            }

            return ret;
        });
        // if(attribute.AttrName === 'Test_Date_2' || attribute.AttrName === 'Test_Date' || attribute.AttrName === 'Street'){
        //     console.log('AAA ',attributes);
        // }
        return attributes;
    }

    vm.formatAttributeValue = function (attribute, rules) {
        var label = attribute.TopBkt ? attribute.TopBkt.Lbl : attribute.Value;

        if (!vm.cube || !rules || rules.length == 0) {
            return label ? label : '*';
        }

        var cube = vm.cube.data[attribute.Entity].Stats[attribute.Attribute];
        var cubeMatches = [];
        var cubeLabels = [];
        var matches = [];
        var filtered = rules.filter(function (item) {
            var Lbl = item.bucketRestriction.bkt.Lbl;

            cubeLabels.push(Lbl);
            return Lbl == label;
        });


        if (cube.Bkt) {
            matches = cube.Bkts.List.filter(function (item) {
                if (cubeLabels.indexOf(item.Lbl) >= 0) {
                    cubeMatches.push(item);
                }
                return item.Lbl == label;
            });
        }

        if (filtered.length != rules.length && rules.length > 1) {
            return 'MULTIPLE USES';
        }

        if (filtered.length == 0 && cubeMatches.length == 0 && cubeMatches.length == 0) {
            return 'CUSTOMIZED';
        }

        if (matches.length >= 1 && filtered.length == 0) {
            return 'SELECTED';
        }

        return label;
    }

    vm.formatAttributeRecords = function (type, value) {
        switch (type) {
            case 'MULTIPLE USES':
                return '*';
            case 'CUSTOMIZED':
                return '*';
            case 'SELECTED':
                return "*";
            default:
                return value;
        }
    }

    vm.isSpecialAttributeValue = function (value) {
        var map = ['SELECTED', 'CUSTOMIZED', 'MULTIPLE USES'];
        return map.indexOf(value) >= 0;
    }

    vm.hideLookupResponse = function () {
        return LookupStore.hideLookupResponse(LookupResponse);
    }

    vm.init();
};
