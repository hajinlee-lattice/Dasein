angular.module('lp.playbook.wizard.targets', [])
.controller('PlaybookWizardTargets', function(
    $state, $stateParams, ResourceUtility, PlaybookWizardStore, DefaultSelectedObject, SelectedSegment, QueryStore, BrowserStorageUtility
) {
    var vm = this;

    angular.extend(vm, {
        stateParams: $stateParams,
        QueryStore: QueryStore,
        authToken: BrowserStorageUtility.getTokenDocument(),
        selectedObjectType: DefaultSelectedObject,
        segment: SelectedSegment,
        columns: QueryStore.columns[DefaultSelectedObject],
        records: QueryStore.getRecordsForUiState(DefaultSelectedObject),
        filtered: [],
        queryText: '',
        sortKey: null,
        sortDesc: true,
        current: 1,
        pagesize: 8,
        max: 0
    });

    vm.init = function() {

    };

    vm.selectObjectType = function(objectType) {
        vm.selectedObjectType = objectType;
        vm.columns = QueryStore.columns[objectType];
        vm.records = QueryStore.getRecordsForUiState(objectType);
        vm.current = 1;
    };

    vm.sortBy = function(key) {
        if (vm.sortKey === key) {
            vm.sortDesc = !vm.sortDesc;
        } else {
            vm.sortKey = key;
            vm.sortDesc = true;
        }
    };

    vm.init();
});