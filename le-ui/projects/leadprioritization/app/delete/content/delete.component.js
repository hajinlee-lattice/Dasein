angular.module('lp.delete.entry', [])
.component('deleteEntry', {
    templateUrl: 'app/delete/content/delete.component.html',
    controller: function( 
        $state, $stateParams, $scope, DeleteDataStore, DeleteDataService, ImportStore, ModalStore, BrowserStorageUtility) 
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
            ClientSession: BrowserStorageUtility.getClientSession()
        });

        vm.init = function() {
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
            vm.toggleModal();
        }

        vm.callbackChangedValue = function (type, position, value) {
            vm.startTime = position == 0 ? value : vm.startTime;
            vm.endTime = position == 1 ? value : vm.endTime;
        }

        vm.getPeriodTimeConfString = function () {
            vm.periodTimeConf.from.visible = vm.showFromTime;
            vm.periodTimeConf.to.visible = vm.showToTime;
            var ret = JSON.stringify(vm.periodTimeConf);
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
                        vm.setBannerMsg(false, true);
                        setTimeout(vm.reset, 0);
                    } else {
                        vm.setBannerMsg(false, false); // show default error message
                    }
                    vm.submittingJob = false;                
                });
            }
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

        vm.initModalWindow = function () {
            vm.modalConfig = {
                'name': "delete_data",
                'type': 'sm',
                'title': 'Warning',
                'titlelength': 100,
                'dischargetext': 'Cancel',
                'dischargeaction': 'cancel',
                'confirmtext': 'Yes, Delete',
                'confirmaction': 'proceed',
                'icon': 'fa fa-exclamation-triangle',
                'iconstyle': {'color': 'white'},
                'confirmcolor': 'blue-button',
                'showclose': true,
                'headerconfig': {'background-color':'#FDC151', 'color':'white'},
                'confirmstyle' : {'background-color':'#FDC151'}
            };
            vm.modalCallback = function (args) {
                if (vm.modalConfig.dischargeaction === args.action) {
                    vm.toggleModal();
                } else if (vm.modalConfig.confirmaction === args.action) {
                    vm.toggleModal();
                    vm.submitCleanupJob();

                }
            }

            vm.toggleModal = function () {
                var modal = ModalStore.get(vm.modalConfig.name);
                if (modal) {
                    modal.toggle();
                }
            }

            vm.setBannerMsg = function(showWarning, showSuccess) {
                vm.showWarningMsg= showWarning;
                vm.showSuccessMsg = showSuccess;
            }

            $scope.$on("$destroy", function () {
                ModalStore.remove(vm.modalConfig.name);
            });
        }

        vm.initModalWindow();

        vm.init();
    }
});