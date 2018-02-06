angular.module('common.wizard.header', [])
.controller('WizardHeader', function(
    $state, $stateParams, $scope, $timeout, $rootScope, ResourceUtility, WizardProgressItems,
    WizardProgressContext, WizardValidationStore, RatingsEngineStore, WizardHeaderTitle, WizardCustomHeaderSteps
) {
    var vm = this;

    angular.extend(vm, {
    	title: WizardHeaderTitle || '',
    	showEntityCounts: false
   	});

    vm.init = function() {
    	if (vm.showCustomHeader()) {
    		vm.setEntityCounts();
    	}
    }

    vm.showCustomHeader = function() {
    	var check = false;
    	var wizardState = 'home.' + WizardProgressContext;

    	WizardCustomHeaderSteps.forEach(function(step) {
    		if ($state.current.name == wizardState + step) {
    			check = true;
    		}
    	});
    	return check;
    }

    vm.setEntityCounts = function() {
        var currentSegment = RatingsEngineStore.getSegment();
        if (currentSegment.accounts != undefined && currentSegment.contacts != undefined) {
	        vm.accounts = currentSegment.accounts.toLocaleString('en');
	        vm.contacts = currentSegment.contacts.toLocaleString('en');
        } else if ($stateParams.rating_id) {
            RatingsEngineStore.getRating($stateParams.rating_id).then(function (rating) {
                vm.accounts = rating.segment.accounts.toLocaleString('en');
                vm.contacts = rating.segment.contacts.toLocaleString('en');
            });
        }
	}


    vm.init();
});