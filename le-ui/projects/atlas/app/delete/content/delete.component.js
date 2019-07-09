angular.module('lp.delete.entry', [])
.component('deleteEntry', {
    templateUrl: 'app/delete/content/delete.component.html',
    controller: function( 
        $state, $stateParams, $scope, $location, DeleteDataStore, DeleteDataService, ImportStore, Banner, Modal, BrowserStorageUtility) 
    { 
        var vm = this,
            resolve = $scope.$parent.$resolve,
            EntitiesCount = resolve.EntitiesCount;

        angular.extend(vm, {
            uploading: false,
            showSuccessMsg: false,
            showWarningMsg: true,
            showFromTime: true,
            showToTime: true,
            submittingJob: false,
            currentTab: 'account',
            deleteWarningMsg: 'Once the delete action is submitted, it can’t be undone.',
            uploadParams: DeleteDataStore.getFileUploadParams(),
            fileName: '',
            counts: {
                'accounts': EntitiesCount.AccountCount,
                'contacts': EntitiesCount.ContactCount,
                'transactions': EntitiesCount.TransactionCount
            },
            periodTimeConf: {
                from: { name: 'from-time', initial: undefined, position: 0, type: 'Time', visible: true },
                to: { name: 'to-time', initial: undefined, position: 1, type: 'Time', visible: true }
            },
            bucketRestriction: {
               "attr":"delete",
            },
            uploadOperations: ['BYUPLOAD_ID', 'BYUPLOAD_ACPD', 'BYUPLOAD_MINDATEANDACCOUNT'],
            ClientSession: BrowserStorageUtility.getClientSession(),
            modalConfigName: 'delete_data'
        });

        vm.init = function() {
            Banner.warning({title: 'Delete Operation', message: 'Once the delete action is submitted, it can’t be undone.'});
            vm.startTime = '';
            vm.endTime = '';
            vm.cleanupOperationType = {
                'account': '',
                'contact': '',
                'transaction': ''
            };
            vm.isValid = {
                'account': false,
                'contact': false,
                'transaction': false
            };
        }

        vm.getForm = function () {
            return $scope.delete_form;
        }

        vm.setEntity = function(type, goState) {
            if (!vm.uploading) { // prevent changing tabs when file is uploading
                clearPreviousSelection();
                vm.cleanupOperationType[vm.currentTab] = '';
                vm.isValid[vm.currentTab] = false;

                vm.currentTab = goState || type.toLowerCase();
                vm.isValid[vm.currentTab] = false;
            }
        }

        vm.setCleanupType = function(option) {
            clearPreviousSelection(option);

            vm.cleanupOperationType[vm.currentTab] = option;
            if (vm.getCleanupType() == 'ALLDATA') {
                vm.isValid[vm.currentTab] = true;
            } else {
                vm.isValid[vm.currentTab] = false;
            }
        }

        vm.getCleanupType = function() {
            var type = vm.cleanupOperationType[vm.currentTab];
            switch (type) {
                case 'BYUPLOAD_ACPD_1':
                case 'BYUPLOAD_ACPD_2':
                    return 'BYUPLOAD_ACPD';
                default:
                    return type;
            }
        }

        vm.getSchema = function() {
            var cleanupType = vm.getCleanupType();
            var isUploadOperation = vm.uploadOperations.indexOf(cleanupType) >= 0;
            switch (vm.currentTab) {
                case 'account':
                    return isUploadOperation ? 'DeleteAccountTemplate' : 'Account';
                case 'contact':
                    return isUploadOperation ? 'DeleteContactTemplate' : 'Contact';
                case 'transaction':
                    return isUploadOperation ? 'DeleteTransactionTemplate' : 'Transaction';
            }
        }

        vm.getEntityImage = function(entity, ico_name) {
            ico_name = !ico_name ? entity : ico_name;
            return vm.currentTab == entity ? '/assets/images/ico-' + ico_name + 's-white.png' : '/assets/images/ico-' + ico_name + 's-dark.png';
        }

        vm.fileLoad = function(headers) {
            vm.uploading = true;
        }

        vm.fileSelect = function(fileName) {
            if (vm.currentTab == 'account' || vm.currentTab == 'contact') {
                vm.uploadParams['BYUPLOAD_ID'].schema = vm.getSchema();
            }
        }

        vm.fileDone = function(result) {
            vm.uploading = false;

            if (result.Result) {
                vm.fileName = result.Result.name;
                vm.isValid[vm.currentTab] = vm.fileName != '';
            } else {
                vm.showWarningMsg = false;
            }
        }
        
        vm.fileCancel = function() {
            vm.uploading = false;
            var xhr = ImportStore.Get('cancelXHR', true);
            
            if (xhr) {
                xhr.abort();
            }
        }

        vm.disableSubmit = function() {
            if (!hasAccessRights() || vm.submittingJob) {
                return true;
            }
            return vm.getCleanupType() != 'BYDATERANGE' ? !vm.isValid[vm.currentTab] : !$scope.delete_form || ($scope.delete_form['from-time'].$invalid || $scope.delete_form['to-time'].$invalid);
        }

        function hasAccessRights() {
            return vm.ClientSession.AccessLevel == 'INTERNAL_ADMIN' || vm.ClientSession.AccessLevel == 'EXTERNAL_ADMIN' || vm.ClientSession.AccessLevel == 'SUPER_ADMIN';
        }

        vm.click = function() {
            Modal.warning({
                name: "delete_data",
                title: "Warning",
                message: 'Are you sure you want to Delete these data? Once the delete job is submitted, it can’t be undone.',
                confirmtext: 'Yes, Delete'
            }, vm.modalCallback)
        }

        vm.callbackChangedValue = function (type, position, value) {
            vm.startTime = position == 0 ? value : vm.startTime;
            vm.endTime = position == 1 ? value : vm.endTime;
        }

        vm.getPeriodTimeConfString = function () {
            vm.periodTimeConf.from.visible = vm.showFromTime;
            vm.periodTimeConf.to.visible = vm.showToTime;
            var ret = vm.periodTimeConf;
            return ret;
        }

        vm.submitCleanupJob = function() {
            vm.submittingJob = true;
            var cleanupType = vm.getCleanupType();
            var schema = vm.getSchema();
            var params = {};
            var url = '';

            switch (cleanupType) {
                case 'BYUPLOAD_ID':
                case 'BYUPLOAD_ACPD':
                case 'BYUPLOAD_MINDATEANDACCOUNT':
                    url = 'cleanupbyupload';
                    params = {
                        fileName: vm.fileName,
                        schema: schema,
                        cleanupOperationType: cleanupType
                    };
                    break;
                case 'BYDATERANGE':
                    url = 'cleanupbyrange';
                    params =  {
                        startTime: vm.startTime,
                        endTime: vm.endTime,
                        schema: schema
                    };
                    break;
                case 'ALLDATA':
                    url = 'cleanupall';
                    params = {
                        schema: schema
                    };
                    break;
            }
            if (url != '') {
                DeleteDataService.cleanup(url, params).then(function(result) {
                    if (result && result.Success) {
                        Banner.reset();
                        var href = getDataProcessingPageHref();
                        var bannerMsg = "The delete action will be scheduled to process and analyze after validation. You can track the status from the <a href='" + href + "'> Data Processing Job page. </a>";
                        Banner.success({title: "Success! Delete Action has been submitted.", message: bannerMsg});

                        setTimeout(vm.reset, 0);
                    }
                    vm.submittingJob = false;                
                });
            }
        }

        function getDataProcessingPageHref() {   
            var url = $location.absUrl().split('/');
            url[url.length - 1] = "jobs/status/data";
            return url.join("/");   
        }

        vm.reset = function() {
            clearPreviousSelection();

            vm.init();

            $scope.$apply();
        }

        function clearPreviousSelection (option) {
            if (vm.uploadOperations.indexOf(vm.getCleanupType()) >= 0) {
                vm.fileName = '';
                var cleanupType = vm.cleanupOperationType[vm.currentTab];
                vm.uploadParams[cleanupType].scope.cancel();
            } else if (vm.getCleanupType() == 'BYDATERANGE' && option != 'BYDATERANGE') {
                vm.resetDatePicker();
                $scope.delete_form['from-time'].$pristine = true;
                $scope.delete_form['to-time'].$pristine = true;
                vm.startTime = '';
                vm.endTime = '';
            }
        }

        vm.modalCallback = function (args) {
            var modal = Modal.get(vm.modalConfigName);
            if (args.action === 'cancel') {
                Modal.modalRemoveFromDOM(modal, args);
            } else if (args.action === 'ok') {
                Modal.modalRemoveFromDOM(modal, args);

                vm.submitCleanupJob();
            }
        }

        $scope.$on("$destroy", function() {
            var modal = Modal.get(vm.modalConfigName);
            if (modal) {
                Modal.modalRemoveFromDOM(modal, {name: 'delete_data'});
            }
        });

        vm.init();
    }
});