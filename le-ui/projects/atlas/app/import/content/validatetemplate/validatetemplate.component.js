import { format } from "path";

angular.module('lp.import.wizard.validatetemplate', [])
.controller('ImportWizardValidateTemplate', function(
    $state, $stateParams, $scope, ResourceUtility, 
    ImportWizardStore, FieldDocument, TemplateData, Validation
) {
    var vm = this;

    angular.extend(vm, {
        fieldDocument: FieldDocument,
        templateData: TemplateData,
        validation: Validation
    });

    vm.init = function() {
        if (vm.validation.length == 0) {
            ImportWizardStore.setValidation('validation', true);
        } else {

            const validationResponse = vm.validation;
            const filterErrors = validationResponse.filter(response => response.status == 'ERROR');
            vm.hasErrors = filterErrors.length == 0 ? false : true;

            if (vm.hasErrors) {
                console.log(vm.validation);
                ImportWizardStore.setValidationStatus(vm.validation);
            } else {
                ImportWizardStore.setValidation('validation', true);
            }
        }
    };

    vm.init();
});