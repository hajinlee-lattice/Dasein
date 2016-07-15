angular.module('lp.enrichment.leadenrichment', [])
.controller('EnrichmentController', function($filter, EnrichmentStore, EnrichmentData, EnrichmentService){
    var vm = this;

    angular.extend(vm, {
        button_save: 'save',
        button_select: 'select for enrichments',
        button_deselect: 'deselect',
        categoryOption: null,
        selectedToggle: null,
        category: null,
        userSelectedCount: 0,
        selectDisabled: 1,
        saveDisabled: 1,
        selectedCount: 0
    });

    vm.changeCategory = function(){
        vm.category = vm.categoryOption;
    }

    vm.selectEnrichment = function(enrichment){
        vm.saveDisabled = 0;
        vm.selectDisabled = 0;

        if (enrichment.IsSelected){
            vm.userSelectedCount++;
        } else {
            vm.userSelectedCount--;
            // this may affect your existing model <- msg, not alert, banner
        }
        if(vm.userSelectedCount < 1) {
            vm.selectDisabled = 1;
        }
    }

    vm.saveSelected = function(){
        var selectedObj = $filter('filter')(vm.enrichments, {'isDirty': true, 'IsSelected': true}),
            deselectedObj = $filter('filter')(vm.enrichments, {'isDirty': true, 'IsSelected': false}),
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
        });
    }

    vm.init = function() {
        EnrichmentStore.getCategories().then(function(result){
            vm.categories = result.data;
            angular.extend(vm, result);
        });

        EnrichmentStore.getEnrichments().then(function(result){
            vm.enrichments = result.data;
            vm.selectedCount = $filter('filter')(vm.enrichments, {'IsSelected': true}).length;
            vm.userSelectedCount = vm.selectedCount;
            vm.selectDisabled = (vm.selectedCount ? 0 : 1);

            angular.extend(vm, result);
        });
    }

    vm.init();

});
