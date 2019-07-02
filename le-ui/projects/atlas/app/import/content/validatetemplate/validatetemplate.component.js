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

        ImportWizardStore.setValidation('validation', false);

        console.log(vm.fileName);
        console.log(vm.templateData);
        console.log(vm.fieldDocument);

        ImportWizardService.validateTemplate(vm.fileName, vm.templateData, vm.fieldDocument).then(function(result) {
            vm.validation = result;
            vm.validating = false;

            if (vm.validation.length == 0) {
                ImportWizardStore.setValidation('validation', true);
                ImportWizardStore.setValidationStatus(null);
            } else {

                const validationResponse = vm.validation;

                vm.errors = validationResponse.filter(response => response.status == 'ERROR');
                vm.hasErrors = vm.errors.length == 0 ? false : true;

                vm.warnings = validationResponse.filter(response => response.status == 'WARNING');
                vm.hasWarnings = vm.warnings.length == 0 ? false : true;

                if (vm.hasErrors) {
                    ImportWizardStore.setValidationStatus(vm.errors);
                } else {
                    ImportWizardStore.setValidation('validation', true);
                }
            }
        });

    };

    vm.init();
});