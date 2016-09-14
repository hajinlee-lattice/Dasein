// green msg after save, 3s
// grid view multple of 12, dynamic across
angular.module('lp.enrichment.leadenrichment', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('EnrichmentController', function($scope, $filter, $timeout, $window, $document, BrowserStorageUtility,
    EnrichmentStore, EnrichmentService, EnrichmentData, EnrichmentCategories, EnrichmentPremiumSelectMaximum){
    var vm = this,
        across = 3, // how many across in grid view
        approximate_pagesize = 25,
        pagesize = Math.round(approximate_pagesize / across) * across;

    angular.extend(vm, {
        label: {
            total: 'Total',
            premium: 'Premium',
            button_save: 'Save',
            button_select: 'Disabled',
            button_selected: 'Enabled',
            button_deselect: 'Enabled',
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
        categoryOption: null,
        metadata: EnrichmentStore.metadata,
        authToken: BrowserStorageUtility.getTokenDocument(),
        category: null,
        userSelectedCount: 0,
        selectDisabled: 1,
        saveDisabled: 1,
        selectedCount: 0,
        pagesize: pagesize,
        across: across,
        initialized: false,
        status_alert: {},
        enrichments: [],
        enable_grid: true,
        view: 'list'
    });

    vm.changeCategory = function(){
        vm.category = vm.categoryOption;
    }
    vm.categoryClass = function(category){
        var category = category.toLowerCase().replace(' ','-');
        return category;
    }

    vm.selectEnrichment = function(enrichment){
        vm.saveDisabled = 0;
        vm.selectDisabled = 0;
        var selectedTotal = $filter('filter')(vm.enrichments, {'IsSelected': true}).length;
        if(selectedTotal > vm.generalSelectLimit) {
            enrichment.IsSelected = false;
            enrichment.IsDirty = false;
            enrichment.button_select = vm.label.generalTotalSelectError;
            enrichment.button_error = true;
            $timeout(function(){
                enrichment.button_select = vm.label.button_select;
                enrichment.button_error = false;
            }, 3000);
            return false;
        }
        if(enrichment.IsPremium) {
            var premiums = $filter('filter')(vm.enrichments, {'IsPremium': true, 'IsSelected': true}).length;
            if(premiums > vm.premiumSelectLimit) {
                enrichment.IsSelected = false;
                enrichment.IsDirty = false;
                enrichment.button_select = vm.label.premiumTotalSelectError;
                enrichment.button_error = true;
                $timeout(function(){
                    enrichment.button_select = vm.label.button_select;
                    enrichment.button_error = false;
                }, 3000);
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
        vm.statusMessage(vm.label.saving_alert, {wait: 0});
        EnrichmentService.setEnrichments(data).then(function(result){
            vm.saveDisabled = true;
            vm.statusMessage(vm.label.saved_alert, {type: 'saved'});
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

    var _scrolled = function() {
        var el = document.querySelector('.subheader-container'),
            $el = angular.element(el),
            watched_el = document.querySelector('.summary .nav'),
            $watched_el = angular.element(watched_el),
            top = watched_el.getBoundingClientRect().top + $watched_el.height(),
            enrichments_list = document.querySelector('.enrichments');

        if(top < 0) {
            $el.addClass('fixed');
            $el.css({width:enrichments_list.offsetWidth});
        } else {
            $el.removeClass('fixed');
            $el.css({width:'auto'});
        }
    }

    var _resized = function(event, wait) {
        var wait = wait || 0;
        $timeout(function(){
            var container = document.querySelector('.subheader-container'),
                height = container.offsetHeight;
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
        }, wait);
    }

    var scrolled = _.throttle(_scrolled, 120);
    var resized = _.throttle(_resized, 120);
    $scope.$on('sidebar:toggle', function(event) {
        _resized(event, 100);
    });

    vm.init = function() {
        _resized();
        vm.enrichments = EnrichmentData.data;
        vm.categories = EnrichmentCategories.data;
        vm.premiumSelectLimit = (EnrichmentPremiumSelectMaximum.data && EnrichmentPremiumSelectMaximum.data['HGData_Pivoted_Source']) || 10;
        vm.generalSelectLimit = 100;
        vm.statusMessageBox = angular.element('.status-alert');

        angular.element($window).bind("scroll", scrolled);
        angular.element($window).bind("resize", resized);
    }

    vm.init();
});
