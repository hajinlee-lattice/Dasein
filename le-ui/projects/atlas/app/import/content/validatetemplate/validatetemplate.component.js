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
        // console.log(vm.fieldDocument);
        // console.log(vm.templateData);
        console.log(vm.validation);

        if (vm.validation.length == 1 && vm.validation[0].status == 'SUCCESS') {
            ImportWizardStore.setValidation('validation', true);
        } else {
            ImportWizardStore.setValidationStatus(vm.validation);
        }
    };

    vm.init();
});