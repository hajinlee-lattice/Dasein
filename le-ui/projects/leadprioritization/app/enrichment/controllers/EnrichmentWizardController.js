// grid view multple of 12 (24), dynamic across
angular.module('lp.enrichmentwizard.leadenrichment', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('EnrichmentWizardController', function($scope, $filter, $timeout, $interval, $window, $document, $q,
    BrowserStorageUtility, FeatureFlagService, EnrichmentStore, EnrichmentService, EnrichmentCategories, EnrichmentPremiumSelectMaximum){

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
            button_select: 'Scoring Enrichment Disabled',
            button_selected: 'Scoring Enrichment Enabled',
            button_deselect: 'Scoring Enrichment Enabled',
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
        enabledManualSave: false,
        enrichments_loaded: false,
        enrichments_completed: false,
        enrichments: [],
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
        subcategories: [],
        categories: [],
        selected_categories: {},
        enable_category_dropdown: false,
        show_internal_filter: FeatureFlagService.FlagIsEnabled(flags.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES),
        enable_grid: true,
        view: 'list'
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

    vm.download_button = {
        label: 'Download',
        class: 'orange-button select-label',
        icon: 'fa fa-chevron-down',
        iconclass: 'orange-button select-more',
        iconrotate: true
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
        stopGetEnrichments = true; // if you leave the page mid-chunking of enrichments this will stop the promise
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

        opts.max = (vm.enrichments.length ? enrichment_chunk_size : 100);

        var max = opts.max,
            offset = opts.offset || 0,
            _store;


        EnrichmentStore.getEnrichments(opts).then(function(result) {
            if (result != null && result.status === 200) {
                vm.enrichments_loaded = true;
                vm.enrichments = vm.enrichments.concat(result.data);
                for(var i in result.data) {
                    var _result = result.data[i];
                    if(vm.enrichmentsObj[_result.Category]) {
                        vm.enrichmentsObj[_result.Category].push(result.data[i]);
                    } else if(_result.Category){
                        vm.enrichmentsObj[_result.Category] = [];
                        vm.enrichmentsObj[_result.Category].push(result.data[i]);
                    }
                }
                numbersNumber = 0;

                _store = result; // just a copy of the correct data strucuture and properties for later
                if(stopGetEnrichments) {
                    vm.enrichments = []; // unset enrichments
                    stopNumbersInterval();
                    return false; // stop getting enrichments
                }
                if(result.data.length === max) {
                    getEnrichmentData({max: max, offset: offset + max});
                } else {
                    _store.data = vm.enrichments; // so object looks like what a typical set/get in the store wants with status, config, etc
                    EnrichmentStore.setEnrichments(_store); // we do the store here because we only want to store it when we finish loading all the attributes
                    stopNumbersInterval();
                    vm.enrichments_completed = true;
                    vm.hasSaved = $filter('filter')(vm.enrichments, {'IsDirty': true}).length;
                }
            }
            var selectedTotal = $filter('filter')(vm.enrichments, {'IsSelected': true});
            vm.generalSelectedTotal = $filter('filter')(selectedTotal, {'IsSelected': true}).length;
            vm.premiumSelectedTotal = $filter('filter')(selectedTotal, {'IsPremium': true, 'IsSelected': true}).length;
        });
    }

    vm.enrichmentsObj = {};
    var getEnrichmentCategories = function() {
        EnrichmentStore.getCategories().then(function(result) {
            vm.categories = result.data;
            _.each(vm.categories, function(value, key){
                getEnrichmentSubcategories(value);
                vm.enrichmentsObj[value] = [];
            });
            vm.enable_category_dropdown = true;
        });
    }

    var getEnrichmentSubcategories = function(category) {
        if(category) {
            EnrichmentStore.getSubcategories(category).then(function(result) {
                if(result.data.length > 1){
                    vm.subcategories[category] = result.data;
                    vm.test = vm.subcategories;
                }
            });
        }
    }

    var textSearch = function(haystack, needle, case_insensitive) {
        var case_insensitive = (case_insensitive === false ? false : true);
        if(case_insensitive) {
            var haystack = haystack.toUpperCase(),
            needle = needle.toUpperCase();
        }
        return haystack.includes(needle);
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
        var selectedTotal = $filter('filter')(vm.enrichments, {'IsSelected': true});
        vm.generalSelectedTotal = $filter('filter')(selectedTotal, {'IsSelected': true}).length;
        //vm.generalSelectedTotal = $filter('filter')(vm.enrichments, {'IsSelected': true}).length;
        if(vm.generalSelectedTotal > vm.generalSelectLimit) {
            enrichment.IsSelected = false;
            enrichment.IsDirty = false;
            vm.statusMessage(vm.label.generalTotalSelectError);
            return false;
        }
        if(enrichment.IsPremium) {
            //vm.premiumSelectedTotal = $filter('filter')(vm.enrichments, {'IsPremium': true, 'IsSelected': true}).length;
            vm.premiumSelectedTotal = $filter('filter')(selectedTotal, {'IsPremium': true, 'IsSelected': true}).length;
            if(vm.premiumSelectedTotal > vm.premiumSelectLimit) {
                enrichment.IsSelected = false;
                enrichment.IsDirty = false;
                vm.statusMessage(vm.label.premiumTotalSelectError);
                return false;
            }
        }
        if (enrichment.IsSelected){
            vm.userSelectedCount++;
            vm.statusMessage(vm.label.changed_alert);
        } else {
            vm.userSelectedCount--;
            if(!enrichment.WasDirty) {
                enrichment.WasDirty = true;
                vm.disabled_count = $filter('filter')(vm.enrichments, {'IsDirty': true, 'IsSelected': false}).length;
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
        var selectedObj = $filter('filter')(vm.enrichments, {'IsDirty': true, 'IsSelected': true}),
            deselectedObj = $filter('filter')(vm.enrichments, {'IsDirty': true, 'IsSelected': false}),
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
                var dirtyObj = $filter('filter')(vm.enrichments, {'IsDirty': true});
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
                    /*
                    target.on('mouseover', function(){
                        open_subcategory();
                    });
                    */
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

    var subcategoryCountList = [];
    vm.subcategoryCount = function(category, subcategory) {
        var filtered = vm.enrichmentsObj[category];
        filtered =  $filter('filter')(filtered, {
            'IsSelected': (!vm.metadata.toggle.show.selected ? '' : true),
            'IsPremium': (!vm.metadata.toggle.show.premium ? '' : true) || (!vm.metadata.toggle.hide.premium ? '' : false),
            'IsInternal': (!vm.metadata.toggle.show.internal ? '' : true),
            'Category': category, 
            'Subcategory': subcategory
        });
        filtered = $filter('filter')(filtered, vm.searchFields);
        return filtered.length;
    }

    vm.subcategoryFilter = function(subcategory) {
        //return true;
        if(!vm.enrichments_completed) {
            return true;
        }
        var category = vm.category;
        return (vm.subcategoryCount(category, subcategory) ? true : false);
    }

    vm.categoryIcon = function(category){
        var path = '/assets/images/enrichments/',
            category = subcategoryRenamer(category, '-'),
            icon = 'ico-attr-' + category + '.png';

        return path + icon;
    }

    vm.categoryCount = function(category) {
        var filtered = vm.enrichmentsObj[category];
        filtered =  $filter('filter')(filtered, {
            'IsSelected': (!vm.metadata.toggle.show.selected ? '' : true),
            'IsPremium': (!vm.metadata.toggle.show.premium ? '' : true) || (!vm.metadata.toggle.hide.premium ? '' : false),
            'IsInternal': (!vm.metadata.toggle.show.internal ? '' : true),
            'Category': category
        });
        filtered = $filter('filter')(filtered, vm.searchFields);
        return filtered.length;
    }

    vm.categoryClick = function(category) {
        var category = category || '';
        if(vm.subcategory && vm.category == category) {
            vm.subcategory = '';
        } else if(vm.category == category) {
            vm.subcategory = '';
            vm.category = '';
        } else {
            vm.subcategory = '';
            vm.category = category;
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

    $scope.$watch('vm.query', function(newvalue, oldvalue){
        if(!vm.category && newvalue) {
            vm.category = vm.categories[0];
        }
    });

    var EnrichmentCount = function() {
        var opts = {};
        EnrichmentStore.getCount(opts).then(function(result) {
            vm.count = result.data;
        });
    }
    vm.percentage = function(number, total) {
        if(number && total) {
            return (total / number) * 100;
        }
        return 0;
    }

    angular.element('.dropdown-container > h2').click(function(e){
        angular.element(this).toggleClass('active');
        angular.element('.dropdown-container ul.dropdown').toggleClass('open');
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

        EnrichmentCount();

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
