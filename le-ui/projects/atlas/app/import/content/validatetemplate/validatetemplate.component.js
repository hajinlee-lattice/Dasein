import { format } from "path";

angular.module('lp.import.wizard.validatetemplate', [])
.controller('ImportWizardValidateTemplate', function(
    $state, $stateParams, $scope, ResourceUtility, 
    ImportWizardService, ImportWizardStore, FieldDocument, TemplateData, FileName
) {
    var vm = this;

    angular.extend(vm, {
        fileName: FileName,
        fieldDocument: FieldDocument,
        templateData: TemplateData,
        validating: true
    });

    vm.init = function() {

        ImportWizardService.validateTemplate(vm.fileName, vm.templateData, vm.fieldDocument).then(function(result) {
            vm.validation = result;
            vm.validating = false;

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
        });

    };

    vm.init();
});