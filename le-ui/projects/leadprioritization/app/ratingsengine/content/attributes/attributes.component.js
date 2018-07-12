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
            vm.scoringAttributes['CustomFileAttributes'] = Rating.activeModel.AI.advancedModelingConfig.custom_event.dataStores ? 
                        Rating.activeModel.AI.advancedModelingConfig.custom_event.dataStores.indexOf('CustomFileAttributes') >= 0 : true;
        } else if ( RatingsEngineStore.getCustomEventModelingType() == 'CDL' || Rating.segment != null) {
            vm.disableTrainingAttributes = true;
            vm.scoringAttributes['CDL'] = Rating.activeModel.AI.advancedModelingConfig.custom_event.dataStores ? 
                        Rating.activeModel.AI.advancedModelingConfig.custom_event.dataStores.indexOf('CDL') >= 0 : true;
            vm.scoringAttributes['CustomFileAttributes'] = false;
        }

        if (Rating.activeModel.AI.advancedModelingConfig.custom_event.dataStores) {
            console.log(Rating.activeModel.AI.advancedModelingConfig.custom_event.dataStores);
            Object.keys(vm.scoringAttributes).forEach(function(attr) {
                vm.scoringAttributes[attr] = Rating.activeModel.AI.advancedModelingConfig.custom_event.dataStores.indexOf(attr) >= 0;
            });  
        }
        var dataStores = filterDataStores(vm.scoringAttributes);
        RatingsEngineStore.setDataStores(dataStores);
        RatingsEngineStore.setValidation("attributes", true);
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
