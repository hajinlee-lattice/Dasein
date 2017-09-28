angular.module('lp.import.wizard.customfields', [])
.controller('ImportWizardCustomFields', function(
    $state, $stateParams, $scope, ResourceUtility, ImportWizardStore
) {
    var vm = this;

    angular.extend(vm, {
        parent_name: $state.current.name.split('.')[3],
        full_name: $state.current.name,
        customFieldsIgnore: [],
        FieldDocument: ImportWizardStore.getFieldDocument(),
        AvailableFields: ImportWizardStore.getAvailableFields(),
        ignoredFields: ImportWizardStore.getFieldDocumentAttr('ignoredFields'),
        fieldMappings: ImportWizardStore.getFieldDocumentAttr('fieldMappings'),
    });

    vm.init = function() {
        vm.size= vm.AvailableFields.lengh;
        vm.customFields = ImportWizardStore.getCustomFields(vm.parent_name);

        vm.customFields.forEach(function(item){
            vm.customFieldsIgnore[item.CustomField] = false;
        });

        vm.toggleIgnores = function($event) {
            var target = $event.target;
            vm.customFields.forEach(function(item){
                vm.customFieldsIgnore[item.CustomField] = target.checked;
            });
        }

    };

    vm.filterStandardList = function(input) {
        for (var i =0 ; i < vm.AvailableFields.length; i++) {
            if (vm.AvailableFields[i] === input.userField) {
                return true;
            }
        }
        return false;
    };
    vm.init();
});