// grid view multple of 12 (24), dynamic across
angular.module('lp.enrichmentwizard.leadenrichment', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('EnrichmentWizardController', function($scope, $filter, $timeout, $interval, $window, $document, $q,
    BrowserStorageUtility, FeatureFlagService, EnrichmentStore, EnrichmentService, EnrichmentCount, EnrichmentCategories, 
    EnrichmentPremiumSelectMaximum, EnrichmentAccountLookup, LookupStore){

    var vm = this,
        across = 4, // how many across in grid view
        approximate_pagesize = 25,
        pagesize = Math.round(approximate_pagesize / across) * across,
        enrichment_chunk_size = 5000;

    var flags = FeatureFlagService.Flags();

    angular.extend(vm, {
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
        lookupMode: EnrichmentAccountLookup !== null,
        lookupFiltered: EnrichmentAccountLookup,
        LookupResponse: LookupStore.response,
        hasCompanyInfo: (LookupStore.response && LookupStore.response.companyInfo ? Object.keys(LookupStore.response.companyInfo).length : 0),
        count: (EnrichmentAccountLookup ? Object.keys(EnrichmentAccountLookup).length : EnrichmentCount.data),
        enabledManualSave: false,
        enrichments_loaded: false,
        enrichments_completed: false,
        enrichmentsObj: {},
        enrichments: [],
        subcategoriesList: [],
        categoryOption: null,
        metadata: EnrichmentStore.metadata,
        authToken: BrowserStorageUtility.getTokenDocument(),
        userSelectedCount: 0,
        selectDisabled: 1,
        saveDisabled: 1,
        selectedCount: 0,
        pagesize: pagesize,
        across: across,
        initialized: false,
        status_alert: {},
        enrichments: [],
        _subcategories: [],
        subcategories: [],
        categories: [],
        selected_categories: {},
        enable_category_dropdown: false,
        show_internal_filter: FeatureFlagService.FlagIsEnabled(flags.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES),
        enable_grid: true,
        view: 'list',
        queryText: '',
        blah: {}
    });

    vm.orders = {
        attribute: 'DisplayName',
        subcategory: 'toString()',
        category: 'toString()'
    }
    vm.sortPrefix = '+';

    vm.sortOrder = function() {
        var sortPrefix = vm.sortPrefix.replace('+','');
        if(!vm.category) {
            return sortPrefix + vm.orders.category;
        } else if(vm.subcategories[vm.category] && vm.subcategories[vm.category].length && !vm.subcategory) {
            return sortPrefix + vm.orders.subcategory;
        } else {
            return sortPrefix + vm.orders.attribute;
        }
    }

    vm.filter = function(items, property, value) {
        for (var i=0, result=[]; i < items.length; i++) {
            if (typeof items[i][property] != 'undefined' && items[i][property] == value) {
                result.push(items[i]);
            }
        }

        return result;
    }

    vm.download_button = {
        //label: 'Download',
        //labelIcon: 'fa-download',
        class: 'orange-button select-label',
        icon: 'fa fa-download',
        iconclass: 'white-button select-more',
        iconrotate: false,
        tooltip: 'Download Enrichments'
    };

    vm.download_button.items = [{ 
        href: '/files/enrichment/lead/downloadcsv?onlySelectedAttributes=false&Authorization=' + vm.authToken,
        label: vm.label.button_download,
        icon: 'fa fa-file-o' 
    },{
        href: '/files/enrichment/lead/downloadcsv?onlySelectedAttributes=true&Authorization=' + vm.authToken,
        label: vm.label.button_download_selected,
        icon: 'fa fa-file-o' 
    }];

    var stopGetEnrichments = false;
    $scope.$on('$destroy', function () {
        // lets load all enrichments anyway incase they go to Account Lookup tool -jlazarus
        //stopGetEnrichments = true; // if you leave the page mid-chunking of enrichments this will stop the promise
        angular.element($window).unbind("scroll", scrolled);
        angular.element($window).unbind("resize", resized);
    });

    var fakeIncrement = false;
    if(fakeIncrement) {
        vm.placeholderTotal;
        var numbersNumber = 0;
        var numbersInterval = $interval(function(){
            var query = angular.element('.subheader .query .ng-search input'),
                filteredTotal = parseInt(query.attr('data-filteredTotal'));

            if(numbersNumber < enrichment_chunk_size && filteredTotal >= 1000) {
                numbersNumber = numbersNumber+1*10;
            }
            vm.placeholderTotal = filteredTotal + numbersNumber;
        }, enrichment_chunk_size / 1000);
    }

    var stopNumbersInterval = function(){
        if(numbersInterval) {
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

        $timeout(function() {
            if (EnrichmentStore.enrichments) {
                vm.xhrResult(EnrichmentStore.enrichments, true);
            } else {
                for (var j=0; j<iterations; j++) {
                    EnrichmentStore.getEnrichments({ max: max, offset: j * max }).then(vm.xhrResult);
                }
            }
        }, 500);
    }

    vm.xhrResult = function(result, cached) {
        var _store, key, item;

        if (cached) {
            vm.enrichmentsObj = {};
            EnrichmentStore.init();
        }

        vm.concurrentIndex++;

        if (result != null && result.status === 200) {
            if (vm.lookupFiltered !== null) {
                for (var i=0, data=[]; i<result.data.length; i++) {
                    if (vm.lookupFiltered[result.data[i].FieldNameInTarget]) {
                        data.push(result.data[i]);
                    }
                }
            } else {
                var data = result.data;
            }

            vm.enrichments_loaded = true;
            vm.enrichmentsStored = vm.enrichments.concat(result.data);
            vm.enrichments = vm.enrichments.concat(data);

            // Updates the Acct Lookup Attributes tab count
            if (vm.lookupMode) {
                LookupStore.add('count', vm.enrichments.length);
            }

            for (key in data) {
                item = data[key];

                if (!vm.enrichmentsObj[item.Category]) {
                    vm.enrichmentsObj[item.Category] = [];
                }

                vm.enrichmentsObj[item.Category].push(item);
            }

            numbersNumber = 0;

            _store = result; // just a copy of the correct data strucuture and properties for later
            if (cached || vm.enrichments.length >= vm.count || vm.concurrentIndex >= vm.concurrent) {
                _store.data = vm.enrichmentsStored; // so object looks like what a typical set/get in the store wants with status, config, etc
                EnrichmentStore.setEnrichments(_store); // we do the store here because we only want to store it when we finish loading all the attributes
                vm.hasSaved = vm.filter(vm.enrichments, 'IsDirty', true).length;
                
                $timeout(function() {
                    vm.enrichments_completed = true;
                }, 500);
            }
        }

        var selectedTotal = vm.filter(vm.enrichments, 'IsSelected', true);
        vm.generalSelectedTotal = selectedTotal.length;
        vm.premiumSelectedTotal = vm.filter(selectedTotal, 'IsPremium', true).length;
    }

   vm.filterLookupFiltered = function(item, type) {
        if(type === 'PERCENTAGE') {
            var percentage = Math.round(item * 100) + '%';
            return percentage;
        }
        return item;
   }

    vm.topAttributes = [];
    var getTopAttributes = function(opts) {
        var opts = opts || {},
            category = opts.category;

        EnrichmentStore.getTopAttributes(opts).then(function(result) {
            vm.topAttributes[category] = result.data;
        });
    }

    var getEnrichmentCategories = function() {
        EnrichmentStore.getCategories().then(function(result) {
            vm.categories = result.data;
            _.each(vm.categories, function(value, key){
                getEnrichmentSubcategories(value);
                getTopAttributes({category: value, loadEnrichmentMetadata: true});
                if (!vm.enrichmentsObj[value]) {
                    vm.enrichmentsObj[value] = [];
                }
            });
            vm.enable_category_dropdown = true;
        });
    }

    var subcategoriesExclude = [];
    var getEnrichmentSubcategories = function(category) {
        if(category) {
            EnrichmentStore.getSubcategories(category).then(function(result) {
                if(result.data.length > 1){
                    var subcategories = result.data;
                    vm._subcategories[category] = subcategories;
                    vm.subcategories[category] = subcategories;
                    if(subcategories.length <= 1) {
                        subcategoriesExclude.push(category);
                    }
                }
            });
        }
    }

    var textSearch = function(haystack, needle, case_insensitive) {
        var case_insensitive = (case_insensitive === false ? false : true);
        if(case_insensitive) {
            var haystack = haystack.toLowerCase(),
            needle = needle.toLowerCase();
        }
        // .indexOf is faster and more supported than .includes
        return (haystack.indexOf(needle) >= 0);
    }

    vm.searchFields = function(enrichment){
        if(vm.query) {
            if(textSearch(enrichment.DisplayName, vm.query)) {
                return true;
            } else if(textSearch(enrichment.Description, vm.query)) {
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    vm.inCategory = function(enrichment){
        if(enrichment.DisplayName && !(_.size(vm.selected_categories))) { // for case where this is used as a | filter in the enrichments ngRepeat on initial state
            return true;
        }
        var selected = (typeof vm.selected_categories[enrichment.Category] === 'object');
        return selected;
    };

    vm.inSubcategory = function(enrichment){
        var category = vm.selected_categories[enrichment.Category],
            subcategories = (category && category['subcategories'] ? category['subcategories'] : []),
            subcategory = enrichment.Subcategory;

        if(enrichment.DisplayName && !subcategories.length) { // for case where this is used as a | filter in the enrichments ngRepeat on initial state
            return true;
        }

        if(!subcategories.length) {
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
        if(vm.generalSelectedTotal > vm.generalSelectLimit) {
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

        if(vm.userSelectedCount < 1) {
            vm.selectDisabled = 1;
        }

        if(!vm.enabledManualSave) {
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

        EnrichmentService.setEnrichments(data).then(function(result){
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

    vm.categoriesDropdown = function($event){
        vm.show_categories = !vm.show_categories;

        var parent_selector = '.dropdown-categories',
            parent = angular.element(parent_selector);

        if(!vm.show_categoires) {
            parent.find('.show-subcategory').removeClass('show-subcategory');
        }

        if($event && $event.target) {
            var sub_targets = parent.find('.subcategory-toggle'),
                categories = parent.find('ul').first();

            categories.css({minWidth: parent.width(), top: parent.height() - 1});

            sub_targets.each(function(key, value){
                var target = angular.element(value),
                    subcategories = target.parent().siblings('ul');

                if(subcategories.length) {
                    target.unbind('click');
                    var open_subcategory = function(){
                        var add = true,
                            subcategories_width = subcategories.outerWidth(),
                            subcategories_top = parent.find('h4').first().outerHeight();

                        if(subcategories.hasClass('show-subcategory')) {
                            add = false;
                        }
                        parent.find('.show-subcategory, .subcategory-toggle').removeClass('show-subcategory');
                        if(add) {
                            subcategories.siblings('.category').find('.subcategory-toggle').addClass('show-subcategory');
                            subcategories.addClass('show-subcategory').css({left: -(subcategories_width), top: -(subcategories_top)});
                        } 
                    }
                    target.on('click', function(){
                        open_subcategory();
                    });
                }
            });

            var click = function($event){
                var clicked = angular.element($event.target),
                inside = clicked.closest(parent).length;
                if(!inside) {
                    parent.find('.show-subcategory').removeClass('show-subcategory');
                    $scope.vm.show_categories = false;
                    $scope.$digest();
                    $document.unbind('click', click);
                }
            }
            $document.bind('click', click);
        }
    }

    var subcategoryRenamer = function(string, replacement){
        if(string) {
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
                || (vm.metadata.toggle.show.selected && !item.IsSelected) 
                || (vm.metadata.toggle.show.premium && !item.IsPremium) 
                || (vm.metadata.toggle.hide.premium && item.IsPremium) 
                || (vm.metadata.toggle.show.internal && !item.IsInternal)) {
                    continue;
                }
                result.push(item);
            }
        }

        return result.length;
    }

    vm.subcategoryFilter = function(subcategory) {
        if(!vm.enrichments_completed) {
            return true;
        }
        var category = vm.category,
            count = vm.subcategoryCount(category, subcategory);

        return (count ? true : false);
    }

    vm.categoryIcon = function(category){
        var path = '/assets/images/enrichments/',
            category = subcategoryRenamer(category, '-'),
            icon = 'ico-attr-' + category + '.png';

        return path + icon;
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
                || (vm.metadata.toggle.show.selected && !item.IsSelected) 
                || (vm.metadata.toggle.show.premium && !item.IsPremium) 
                || (vm.metadata.toggle.hide.premium && item.IsPremium) 
                || (vm.metadata.toggle.show.internal && !item.IsInternal)) {
                    continue;
                }
                result.push(item);
            }
        }

        return result.length;
    }

    vm.categoryClick = function(category) {
        var category = category || '';
        if(vm.subcategory && vm.category == category) {
            vm.subcategory = '';
            if(subcategoriesExclude.includes(category)) { // don't show subcategories
                vm.subcategory = vm.subcategories[category][0];
            }
        } else if(vm.category == category) {
            vm.subcategory = '';
            //vm.category = '';
        } else {
            vm.subcategory = '';
            if(subcategoriesExclude.includes(category)) {
                vm.subcategory = vm.subcategories[category][0];
            }
            vm.category = category;

            vm.filterEmptySubcategories();
        }
    }

    // use to test 
    //vm.LookupResponse.companyInfo = {"LE_COUNTRY":"USA","HEADQUARTER_PARENT_STATE_PROVINCE":"California","HEADQUARTER_PARENT_DnB_COUNTY_CODE":"684","HEADQUARTER_PARENT_DnB_COUNTRY_CODE":"805","DOMESTIC_ULTIMATE_STATE_PROV_ABR":"CA","GLOBAL_ULTIMATE_BUSINESS_NAME":"Lattice Engines, Inc.","REGISTERED_ADDRESS_INDICATOR":"N","GLOBAL_ULTIMATE_COUNTRY_NAME":"USA","DOMESTIC_ULTIMATE_CITY_NAME":"San Mateo","SALES_VOLUME_LOCAL_CURRENCY":39095607,"HEADQUARTER_PARENT_COUNTRY_NAME":"USA","LE_SIC_CODE":"73790200","LDC_Name":"Lattice Engines, Inc.","OUT_OF_BUSINESS_INDICATOR":"0","PRINCIPALS_INCLUDED_INDICATOR":"Y","IMPORT_EXPORT_AGENT_CODE":"G","LDC_Country":"USA","CLUSTER_TPS":"021","GLOBAL_ULTIMATE_DUNS_NUMBER":"028675958","DOMESTIC_ULTIMATE_STATE_PROVINCE":"California","PROPENSITY_TO_HAVE_A_LEASE_ACCOUNT":"01","PREMIUM_MARKETING_PRESCREEN":"5","GLOBAL_ULTIMATE_DnB_CITY_CODE":"007247","HEADQUARTER_PARENT_BUSINESS_NAME":"Lattice Engines, Inc.","DnB_COUNTY_CODE":"684","DnB_CONTINENT_CODE":"6","EMPLOYEES_HERE":85,"TRIPLE_PLAY_SEGMENT":"3","GLOBAL_ULTIMATE_INDICATOR":"Y","GLOBAL_ULTIMATE_POSTAL_CODE":"944044059","PROPENSITY_TO_HAVE_A_LINE_OF_CRE":"01","GLOBAL_ULTIMATE_DnB_CONTINENT_CODE":"6","DnB_STATE_PROVINCE_CODE":"009","LINE_OF_BUSINESS":"Computer related services, nec, nsk","CREDIT_CARD_RESPONSE_R":"09","DOMESTIC_ULTIMATE_DnB_COUNTRY_CODE":"805","EMPLOYEES_HERE_RELIABILITY_CODE":"2","LDC_ZipCode":"944044059","HEADQUARTER_PARENT_STATE_PROV_ABR":"CA","DIAS_CODE":"010017663","YEAR_STARTED":"2010","CHIEF_EXECUTIVE_OFFICER_TITLE":"Chief Executive Officer","LE_EMPLOYEE_RANGE":"101-200","GLOBAL_ULTIMATE_STATE_PROVINCE_NAME":"California","LE_NUMBER_OF_LOCATIONS":5,"LDC_Domain":"lattice-engines.com","DOMESTIC_ULTIMATE_DUNS_NUMBER":"028675958","SUBSIDIARY_INDICATOR":"0","LE_REVENUE_RANGE":"11-50M","HEADQUARTER_PARENT_POSTAL_CODE":"944044059","LDC_Street":"1820 Gateway Dr Ste 200","CLUSTER_CRS":"312","US_1987_SIC_1":"7379","EMPLOYEES_TOTAL":120,"LE_IS_PRIMARY_LOCATION":"Y","TELEPHONE_NUMBER":"8774600010","LAST_UPDATE_DATE":"20160929","HIERARCHY_CODE":"01","RECORD_SOURCE_CODE":"DWB","STATE_PROVINCE_ABBR":"CA","COMPOSITE_RISK_SCORE":"6","HEADQUARTER_PARENT_DUNS_NUMBER":"028675958","GLOBAL_ULTIMATE_CITY_NAME":"San Mateo","LOCAL_ACTIVITY_TYPE_CODE":"000","HEADQUARTER_PARENT_DnB_CITY_CODE":"007247","LE_PRIMARY_DUNS":"028675958","DnB_CITY_CODE":"007247","LE_INDUSTRY":"Computer Related Services, Nec","NUMBER_OF_FAMILY_MEMBERS":5,"GLOBAL_ULTIMATE_STREET_ADDRESS":"1820 Gateway Dr Ste 200","LEASE_BALANCE_RANKING_1_TO_10":"01","LDC_DUNS":"028675958","LDC_State":"California","DOMESTIC_ULTIMATE_STREET_ADDRESS":"1820 Gateway Dr Ste 200","HEADQUARTER_PARENT_STREET_ADDRESS":"1820 Gateway Dr Ste 200","TOTAL_CREDIT_BALANCE_RANKING_CR":"01","GLOBAL_ULTIMATE_DnB_COUNTRY_CODE":"805","HEADQUARTER_PARENT_DnB_CONTINENT":"6","GLOBAL_ULTIMATE_DnB_COUNTY_CODE":"684","EMPLOYEES_TOTAL_RELIABILITY_CODE":"0","SALES_VOLUME_US_DOLLARS":39095607,"SALES_VOLUME_RELIABILITY_CODE":"2","CURRENCY_CODE":"0020","CHIEF_EXECUTIVE_OFFICER_NAME":"Shashi Upadhyay","LDC_City":"San Mateo","DOMESTIC_ULTIMATE_DnB_CITY_CODE":"007247","DOMESTIC_ULTIMATE_POSTAL_CODE":"944044059","LEGAL_STATUS_CODE":"003","DnB_COUNTRY_CODE":"805","COUNTRY_ACCESS_CODE":"0001","LE_COMPANY_PHONE":"8774600010","HEADQUARTER_PARENT_CITY_NAME":"San Mateo","GLOBAL_ULTIMATE_STATE_PROV_ABR":"CA","STATUS_CODE":"1","FULL_REPORT_DATE":"20160929","LE_NAICS_CODE":"541512","DOMESTIC_ULTIMATE_BUSINESS_NAME":"Lattice Engines, Inc.","CABLE_TELEX_NUMBER":"20NMNNANP","LE_IS_PRIMARY_DOMAIN":"Y"};
    vm.companyInfoFormatted = function (type, value) {
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
                    address.push(info.LDC_ZipCode.substr(0,4) + ',');
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

    var _scrolled = function() {
        var el = document.querySelector('.subheader-container');
            if(el) {
            var $el = angular.element(el),
                watched_el = document.querySelector('#mainContentView'),
                $watched_el = angular.element(watched_el),
                top = watched_el.getBoundingClientRect().top,
                enrichments_list = document.querySelector('.filters-enrichments');

                if(top < 0) {
                    $el.addClass('fixed');
                    $el.css({width:enrichments_list.offsetWidth});
                } else {
                    $el.removeClass('fixed');
                    $el.css({width:'auto'});
                }
            }
        }

    var _resized = function(event, wait) {
        var wait = wait || 0;
        $timeout(function(){
            var container = document.querySelector('.subheader-container');
            if(container) {
                var height = container.offsetHeight,
                    enrichments_list = document.querySelector('.enrichments'),
                    subheader = angular.element('.subheader-container');

                if(subheader.hasClass('fixed')) {
                    subheader.css({width:enrichments_list.offsetWidth});
                } else {
                    subheader.css({width:'auto'});
                }

                if(height > 70) {
                    angular.element(container).addClass('wrapped');
                } else {
                    angular.element(container).removeClass('wrapped');
                }
            }
        }, wait);
    }

    var scrolled = _.throttle(_scrolled, 120);
    var resized = _.throttle(_resized, 120);
    $scope.$on('sidebar:toggle', function(event) {
        _resized(event, 100);
    });

    $scope.$watchGroup([
            'vm.metadata.toggle.show.selected', 
            'vm.metadata.toggle.show.premium', 
            'vm.metadata.toggle.hide.premium', 
            'vm.metadata.toggle.show.internal'
        ], function(newValues, oldValues, scope) {
        vm.filterEmptySubcategories();
    });

    $scope.$watch('vm.queryText', function(newvalue, oldvalue){
        vm.queryInProgress = true;

        if (vm.queryTimeout) {
            $timeout.cancel(vm.queryTimeout);
        }

        // debounce timeout to speed things up
        vm.queryTimeout = $timeout(function() {
            if(!vm.category && newvalue) {
                vm.category = vm.categories[0];
            }

            vm.query = vm.queryText;
            
            vm.filterEmptySubcategories();

            vm.queryInProgress = false;
        }, 333);
    });

    var addUniqueToArray = function(array, item) {
        if (array && item && !array.includes(item)) {
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
                vm.subcategory = newCategories[0];
            } else {
                if (subcategoriesExclude.includes(vm.category)) {
                    vm.subcategory = '';
                }
                removeFromArray(subcategoriesExclude, vm.category);
            }
            vm.subcategories[vm.category] = newCategories;
        }
    }

    vm.percentage = function(number, total) {
        if (number && total) {
            return (total / number) * 100;
        }
        return 0;
    }

    var download_buttons = angular.element('.dropdown-container > h2');
    download_buttons.click(function(e){
        var button = angular.element(this),
            toggle_on = !button.hasClass('active');

        download_buttons.removeClass('active');
        download_buttons.siblings('ul.dropdown').removeClass('open');

        if(toggle_on) {
            button.addClass('active');
            button.siblings('ul.dropdown').addClass('open');
        }

        e.stopPropagation();

    });

    angular.element(document).click(function(event) {
        var target = angular.element(event.target),
        el = angular.element('.dropdown-container ul.dropdown'),
        has_parent = target.parents().is('.dropdown-container'),
        is_visible = el.is(':visible');
        if(!has_parent) {
            el.removeClass('open');
            el.siblings('.button.active').removeClass('active');
        }
    });

    var c = 0;
    var debugCounter = function(){
        c++;
        console.log('debugCounter: ', c);
    }

    vm.init = function() {
        _resized();

        getEnrichmentData();

        getEnrichmentCategories();

        vm.premiumSelectLimit = (EnrichmentPremiumSelectMaximum.data && EnrichmentPremiumSelectMaximum.data['HGData_Pivoted_Source']) || 10;
        vm.generalSelectLimit = 100;
        vm.statusMessageBox = angular.element('.status-alert');

        angular.element($window).bind("scroll", scrolled);
        angular.element($window).bind("resize", resized);
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
