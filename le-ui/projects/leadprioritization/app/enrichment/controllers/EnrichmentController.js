// lock sub-header
// limit number selectable to (10 for now)
// green msg after save, 3s
angular.module('lp.enrichment.leadenrichment', [])
.controller('EnrichmentController', function($filter, $timeout, $window, $document, EnrichmentStore, EnrichmentService, EnrichmentData, EnrichmentCategories){
    var vm = this;

    angular.extend(vm, {
        button_save: 'Save',
        button_select: 'Turn On',
        button_selected: 'On',
        button_deselect: 'Turn Off',
        deselected_messsage: 'Attribute will be turned off for enrichment',
        categories_see_all: 'See All Categories',
        categories_select_all: 'All Categories',
        premiumSelectError: 'Premium attribute limit reached',
        no_results: 'No enrichments were found',
        categoryOption: null,
        metadata: EnrichmentStore.metadata,
        category: null,
        userSelectedCount: 0,
        selectDisabled: 1,
        saveDisabled: 1,
        selectedCount: 0,
        premiumSelectLimit: 10,
        pagesize: 25,
        initialized: false,
        enrichments: []
    });

    vm.changeCategory = function(){
        vm.category = vm.categoryOption;
    }

    vm.selectEnrichment = function(enrichment){
        vm.saveDisabled = 0;
        vm.selectDisabled = 0;
        if(enrichment.IsPremium) {
            var premiums = $filter('filter')(vm.enrichments, {'IsPremium': true, 'IsSelected': true}).length;
            console.log(premiums);
            if(premiums > vm.premiumSelectLimit) {
                enrichment.IsSelected = false;
                enrichment.IsDirty = false;
                enrichment.button_select = vm.premiumSelectError;
                enrichment.button_error = true;
                $timeout(function(){
                    enrichment.button_select = vm.button_select;
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
                enrichment.button_select = vm.deselected_messsage;
                enrichment.WasDirty = true;
                enrichment.button_msg = true;
                $timeout(function(){
                    enrichment.button_select = vm.button_select;
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

    var _lockSubheader = function(){
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

    var lockSubheader = _.throttle(_lockSubheader, 100);

    vm.init = function() {
        vm.categories = EnrichmentCategories.data;

        // firefox wont do the animations without this on page init... sigh
        $timeout(function() {
            vm.initialized = true;

            // ng-animate stagger works better if we push items
            for (var i=0; i < EnrichmentData.data.length; i++) {
                vm.enrichments.push(EnrichmentData.data[i]);
            }

            vm.selectedCount = $filter('filter')(vm.enrichments, {'IsSelected': true}).length;
            vm.userSelectedCount = vm.selectedCount;
            vm.selectDisabled = (vm.selectedCount ? 0 : 1);
        }, 150);

        angular.element($window).bind("scroll", lockSubheader);
    }

    vm.init();
});
