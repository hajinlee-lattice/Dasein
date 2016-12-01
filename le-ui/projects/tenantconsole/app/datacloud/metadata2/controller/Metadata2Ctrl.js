angular.module("app.datacloud.controller.Metadata2Ctrl", [
    'ngAnimate',
    'ngSanitize',
    'ngPrettyJson',
    'ui.bootstrap'
])
.controller('Metadata2Ctrl', function ($scope, $state, $stateParams, $timeout, $interval, $uibModal, $filter, MetadataService) {

        var vm = {};

        angular.element('body').addClass('Metadata2Ctrl');
        $scope.$on('$destroy', function() {
            if(angular.element('body').hasClass('Metadata2Ctrl')) {
                angular.element('body').removeClass('Metadata2Ctrl');
            }
        });

        vm.pagesize = 50;
        vm.order = 'ColumnName';
        vm.select_version = [];

        vm.versions = {};
        vm.version = $stateParams.version || '2.0.0';
        vm.metadata = [];
        vm.save_ready = false;

        vm.columns = {
                ColumnName: {label:'Column Name'},
                DisplayName: {label:'Display Name', editable: true},
                Category: {label:'Category', editable: true},
                Subcategory: {label:'Subcategory', editable: true},
                ApprovedUsage: {label:'Approved Usage', editable: true},
                FundamentalType: {label:'Fund Type'},
                StatisticalType: {label:'Stat Type'},
                Description: {label:'Description', editable: true},
                CanEnrich: {label:'Can Enrich', editable: true},
                IsPremium: {label:'Is Premium', editable: true}
            };

        vm.loading = true;
        MetadataService.GetAccountMasterColumns(vm.version).then(function(result){
            if (result.success) {
                vm.metadata = result.resultObj;
                vm.loading = false;
            } else {
                vm.loading_error = result.errMsg;
            }
        });

        MetadataService.GetDataCloudVersions().then(function(result){
            if (result.success) {
                vm.versions = result.resultObj;
            }
        });

        vm.versionChange = function() {
            $state.go('DATACLOUD.METADATA2', {version: vm.select_version});
        };

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

        vm.save = function(){
            var json = [],
                arrays = [];
            for(var j in vm.metadata[0]) {
                if(typeof vm.metadata[0][j] === 'object') {
                    arrays.push(j);
                }
            }
            for(var i in vm.model) {
                var model = vm.model[i],
                    keys = Object.keys(model),
                    metadata = $filter('filter')(vm.metadata, {'ColumnId': model.ColumnId})[0], //vm.metadata[i],
                    intersection = _.intersection(keys, arrays);
                    if(keys.length < 2) {
                        break;
                    }
                if(intersection.length) {
                    for(var k in intersection) {
                        var key = intersection[k];
                        if(model[key]) {
                            model[key] = model[key].split(',');
                        }
                    }
                }
                var merged = Object.assign({}, metadata, model);
                json.push(merged);
            }
            if(json.length) {
                vm.saved_json = json;
            }
            console.log('saving ->', vm.saved_json);
        };

        /**
         * because stupid, and angular has issues with ng-select
         */
        var setOptionsSelectedState = $interval(function(){
            var selected = document.querySelectorAll('option[selected="selected"]'); 
            for(var i in selected) { 
                var select = selected[i]; 
                if(typeof select === 'object') {
                    select.selected = true;}
                }
                if(selected.length) {
                    $interval.cancel(setOptionsSelectedState);
                }
            }, 1000);

        vm.init = function(){
        };

        vm.init();
        $scope.vm = vm;

});
