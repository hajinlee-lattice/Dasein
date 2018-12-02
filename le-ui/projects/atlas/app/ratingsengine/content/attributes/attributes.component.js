angular.module('lp.ratingsengine.wizard.attributes', [])
.controller('RatingsEngineAttributes', function ($q, $state, $stateParams, RatingsEngineStore, Rating) {

    var vm = this;
    angular.extend(vm, {
        disableTrainingAttributes: false,
        disableCDLAttributes: false,
        scoringAttributes: {
        	'DataCloud': true,
        	'CDL': true,
        	'CustomFileAttributes': true
        },
    });

    vm.init = function() {
        if (RatingsEngineStore.getCustomEventModelingType() == 'LPI' || Rating.segment == null) {
            vm.disableCDLAttributes = true;
            vm.scoringAttributes['CDL'] = false;
            vm.scoringAttributes['CustomFileAttributes'] = vm.checkDataStores(Rating) ? 
                        Rating.latest_iteration.AI.advancedModelingConfig.custom_event.dataStores.indexOf('CustomFileAttributes') >= 0 : true;
        } else if ( RatingsEngineStore.getCustomEventModelingType() == 'CDL' || Rating.segment != null) {
            vm.disableTrainingAttributes = true;
            vm.scoringAttributes['CDL'] =  vm.checkDataStores(Rating) ? 
                        Rating.latest_iteration.AI.advancedModelingConfig.custom_event.dataStores.indexOf('CDL') >= 0 : true;
            vm.scoringAttributes['CustomFileAttributes'] = false;
        }
        var dataStores = filterDataStores(vm.scoringAttributes);
        RatingsEngineStore.setDataStores(dataStores);
        RatingsEngineStore.setValidation("attributes", true);
    }

    vm.checkDataStores = function(rating) {
        return rating && rating.latest_iteration && rating.latest_iteration.AI && rating.latest_iteration.AI.advancedModelingConfig && rating.latest_iteration.AI.advancedModelingConfig.custom_event
                && rating.latest_iteration.AI.advancedModelingConfig.custom_event.dataStores;
    } 

    vm.setScoringAttributes = function(option) {
    	vm.scoringAttributes[option] = !vm.scoringAttributes[option];
        var dataStores = filterDataStores(vm.scoringAttributes);
        RatingsEngineStore.setDataStores(dataStores);
        dataStores.length > 0 ? RatingsEngineStore.setValidation("attributes", true) : RatingsEngineStore.setValidation("attributes", false);
    }

    var filterDataStores = function (dataStores) {
        return Object.keys(dataStores).filter(function(attr) {return dataStores[attr]});
    }


    vm.init();

});
