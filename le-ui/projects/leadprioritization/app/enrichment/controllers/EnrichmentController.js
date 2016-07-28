// green msg after save, 3s
angular.module('lp.enrichment.leadenrichment', [])
.controller('EnrichmentController', function($scope, $filter, $timeout, $window, $document, EnrichmentStore, EnrichmentService, EnrichmentData, EnrichmentCategories, EnrichmentPremiumSelectMaximum){
    var vm = this;

    angular.extend(vm, {
        label: {
            total: 'Total',
            premium: 'Premium',
            button_save: 'Save',
            button_select: 'Turn On',
            button_selected: 'On',
            button_deselect: 'Turn Off',
            deselected_messsage: 'Attribute will be turned off for enrichment',
            categories_see_all: 'See All Categories',
            categories_select_all: 'All Categories',
            premiumSelectError: 'Premium attribute limit reached',
            no_results: 'No attributes were found',
        },
        categoryOption: null,
        metadata: EnrichmentStore.metadata,
        category: null,
        userSelectedCount: 0,
        selectDisabled: 1,
        saveDisabled: 1,
        selectedCount: 0,
        pagesize: 26, // keep this number even
        initialized: false,
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
        if(enrichment.IsPremium) {
            var premiums = $filter('filter')(vm.enrichments, {'IsPremium': true, 'IsSelected': true}).length;
            if(premiums > vm.premiumSelectLimit) {
                enrichment.IsSelected = false;
                enrichment.IsDirty = false;
                enrichment.button_select = vm.label.premiumSelectError;
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
        } else {
            vm.userSelectedCount--;
            if(!enrichment.WasDirty) {
                enrichment.button_select = vm.label.deselected_messsage;
                enrichment.WasDirty = true;
                enrichment.button_msg = true;
                $timeout(function(){
                    enrichment.button_select = vm.label.button_select;
                    enrichment.button_msg = false;
                }, 3000);
            }
        }
        if(vm.userSelectedCount < 1) {
            vm.selectDisabled = 1;
        }
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
        EnrichmentService.setEnrichments(data).then(function(result){
            vm.saveDisabled = true;
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
            top = watched_el.getBoundingClientRect().top + $watched_el.height();

        if(top < 0) {
            $el.addClass('fixed');
        } else {
            $el.removeClass('fixed');
        }
    }

    var _resized = function(event, wait) {
        var wait = wait || 0;
        $timeout(function(){
            var container = document.querySelector('.subheader-container'),
            height = container.offsetHeight;

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
        vm.premiumSelectLimit = EnrichmentPremiumSelectMaximum.data['HGData_Pivoted_Sourcex'] || 10;

        angular.element($window).bind("scroll", scrolled);
        angular.element($window).bind("resize", resized);

        /*
        $timeout(function() {
            vm.initialized = true;

            for (var i=0; i < EnrichmentData.data.length; i++) {
                vm.enrichments.push(EnrichmentData.data[i]);
            }

            vm.selectedCount = $filter('filter')(vm.enrichments, {'IsSelected': true}).length;
            vm.userSelectedCount = vm.selectedCount;
            vm.selectDisabled = (vm.selectedCount ? 0 : 1);
        }, 150);
        */
    }

    vm.clickPaginate = function() {
        /*
        vm.initialized = false;
        $timeout(function() {
            vm.initialized = true;
        }, 1000)
        */
    }

    vm.init();
});
