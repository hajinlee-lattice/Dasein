angular.module('lp.ratingsengine.wizard.attributes', [])
.controller('RatingsEngineAttributes', function ($q, $state, $stateParams, Rating, RatingsEngineStore) {
    var vm = this;
    console.log(Rating);
    angular.extend(vm, {
        disableTrainingAttributes: false,
        disableCDLAttributes: false,
        scoringAttributes: {
        	'DataCloud': false,
        	'CDL': false,
        	'CustomAttributes': false
        },
        rating: Rating
    });

    vm.init = function() {
        if (RatingsEngineStore.getModelingType() == 'LPI') {
            vm.disableCDLAttributes = true;
        } else if ( RatingsEngineStore.getModelingType() == 'CDL') {
            vm.disableTrainingAttributes = true;
        }
        RatingsEngineStore.setValidation("attributes", false);
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
