angular.module('lp.import.wizard.jobstatus', [])
.controller('ImportWizardJobStatus', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore
) {
    var vm = this;

    angular.extend(vm, {
    	autoImport: true
    });

    vm.init = function() {
        //ImportWizardStore.setValidation('jobstatus', true);

        console.log(vm.autoImport);
    }

    vm.checkForAutoImport = function(){
    	ImportWizardStore.setAutoImport(vm.autoImport);
    }

    vm.init();
});