angular.module('lp.ratingsengine.wizard.attributes', [])
.controller('RatingsEngineAttributes', function ($q, $state, $stateParams, RatingsEngineStore) {

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
        if (RatingsEngineStore.getCustomEventModelingType() == 'LPI') {
            vm.disableCDLAttributes = true;
            vm.scoringAttributes['CDL'] = false;
        } else if ( RatingsEngineStore.getCustomEventModelingType() == 'CDL') {
            vm.disableTrainingAttributes = true;
            vm.scoringAttributes['CustomFileAttributes'] = false;
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
