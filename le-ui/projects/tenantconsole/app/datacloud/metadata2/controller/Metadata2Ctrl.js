angular.module("app.datacloud.controller.Metadata2Ctrl", [
    'kendo.directives',
    'ngAnimate',
    'ngSanitize',
    'ngPrettyJson',
    'ui.bootstrap'
])
.controller('Metadata2Ctrl', function ($scope, $state, $timeout, $uibModal, MetadataService) {

        var vm = {};

        vm.pagesize = 10;
        vm.order = 'ColumnName';

        vm.version = '2.0.0';
        vm.metadta = {};

        vm.columns = {
                ColumnName: {label:'Column Name'},
                DisplayName: {label:'Display Name', editable: true},
                Category: {label:'Category', editable: true},
                Subcategory: {label:'Subcategory', editable: true},
                ApprovedUsage: {label:'Approved Usage'},
                FundamentalType: {label:'Fund Type'},
                StatisticalType: {label:'Stat Type'},
                CanEnrich: {label:'Can Enrich', editable: true},
                Description: {label:'Description', editable: true},
                IsPremium: {label:'Is Premium', editable: true}
            };

        vm.loading = true;
        MetadataService.GetAccountMasterColumns(vm.version).then(function(result){
            if (result.success) {
                vm.metadata = result.resultObj;
                vm.loading = false;
                //console.log(vm.metadata);
            } else {
                vm.loading_error = result.errMsg;
            }
        });

        vm.SortOrder = function(val) {
            return val[vm.order];
        };

        vm.editAll = function() {
            for(var i in vm.edit) {
                vm.edit[i] = false;
            }
            vm.edit_all = !vm.edit_all;
        };

        vm.type = function(key){
            var value = value || '';
            if(['CanEnrich','IsPremium'].includes(key)) {
                return 'checkbox';
            //} else if(['Description'].includes(key)) {
                //return 'textarea';
            }
            return 'default';
        };

        vm.get = function(data) {
            if(data) {
                if(typeof data === 'object') {
                    return data.join(', ');
                }
            }
            return data;
        };

        vm.init = function(){
        };

        vm.init();

        $scope.vm = vm;

});
