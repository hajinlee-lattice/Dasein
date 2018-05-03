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
            showTimeFrame: false,
            submittingJob: false,
            currentTab: 'account',
            deleteWarningMsg: 'Once the delete action is submitted, it can’t be undone.',
            params: DeleteDataStore.getFileUploadParams(),
            counts: {
                'accounts': EntitiesCount.Account,
                'contacts': EntitiesCount.Contact 
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
            vm.uploadedFiles = {
                'account': {
                    'BYUPLOAD_ID': ''
                },
                'contact': {
                    'BYUPLOAD_ID': ''
                },
                'transaction': {
                    'BYUPLOAD_MINDATEANDACCOUNT': '',
                    'BYUPLOAD_ACPD_1': '',
                    'BYUPLOAD_ACPD_2': ''
                }
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

        vm.getDefaultMessage = function()  {
            return "your-" + vm.currentTab + "s.csv";
        }

        vm.callbackChangedValue = function (type, position, value) {
            vm.startTime = position == 0 ? value : vm.startTime;
            vm.endTime = position == 1 ? value : vm.endTime;

            // vm.isValid[vm.currentTab] = ($scope.delete_form['from-time'].$valid && $scope.delete_form['to-time'].$valid);

        }

        vm.changeEntityType = function(type, goState) {
            if (!vm.uploading) { // prevent changing tabs when file is uploading
                vm.clearUploadedFiles(vm.currentTab);
                vm.cleanupOperationType[vm.currentTab] = '';
                vm.isValid[vm.currentTab] = false;
                
                vm.currentTab = goState || type.toLowerCase();
                vm.isValid[vm.currentTab] = false;
                
            }
        }

        vm.fileLoad = function(headers) {
            // console.log('headers', headers);
            vm.uploading = true;
        }

        vm.fileSelect = function(fileName) {
            if (vm.currentTab == 'account' || vm.currentTab == 'contact') {
                vm.params['BYUPLOAD_ID'].schema = vm.getSchema();
            }
        }

        vm.fileDone = function(result) {
            vm.uploading = false;

            if (result.Result) {
                vm.fileName = result.Result.name;
                vm.uploadedFiles[vm.currentTab][vm.cleanupOperationType[vm.currentTab]] = result.Result.name;
                vm.isValid[vm.currentTab] = vm.uploadedFiles[vm.currentTab][vm.cleanupOperationType[vm.currentTab]] != '';
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
            if (!vm.hasAccessRights() || vm.submittingJob) {
                return true;
            }
            return vm.getCleanupType() != 'BYDATERANGE' ? !vm.isValid[vm.currentTab] : !$scope.delete_form || ($scope.delete_form['from-time'].$invalid || $scope.delete_form['to-time'].$invalid);
        }

        vm.hasAccessRights = function() {
            return vm.ClientSession.AccessLevel == 'INTERNAL_ADMIN' || vm.ClientSession.AccessLevel == 'EXTERNAL_ADMIN' || vm.ClientSession.AccessLevel == 'SUPER_ADMIN';
        }

        vm.setCleanupType = function(option) {
            vm.cleanupOperationType[vm.currentTab] = option;
            if (vm.uploadOperations.indexOf(vm.getCleanupType()) >= 0) {
                vm.isValid[vm.currentTab] = vm.uploadedFiles[vm.currentTab][option] != '';
            } else if (vm.getCleanupType() == 'BYDATERANGE') {
                vm.isValid[vm.currentTab] = vm.startTime && vm.endTime && $scope.delete_form['from-time'].$valid;
            } else if (vm.getCleanupType() == 'ALLDATA') {
                vm.isValid[vm.currentTab] = true;
            }
        }

        vm.click = function() {
            vm.toggleModal();
        }

        vm.getPeriodTimeConfString = function () {
            // initDateRange();
            vm.periodTimeConf.from.visible = vm.showFromTime;
            vm.periodTimeConf.to.visible = vm.showToTime;
            var ret = JSON.stringify(vm.periodTimeConf);
            return ret;
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

        vm.getEntityImage = function(entity, ico_name) {
            ico_name = !ico_name ? entity : ico_name;
            return vm.currentTab == entity ? '/assets/images/ico-' + ico_name + 's-white.png' : '/assets/images/ico-' + ico_name + 's-dark.png';
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
                        setTimeout(vm.resetMethod, 0);
                    } else {
                        vm.setBannerMsg(false, false); // show default error message
                    }
                    vm.submittingJob = false;                
                });
            }
        }

        vm.resetMethod = function() {
            
            if (vm.currentTab == 'account' || vm.currentTab == 'contact') {
                vm.params['BYUPLOAD_ID'].scope.cancel();
            }

            if (vm.currentTab == 'transaction') {
                vm.resetDatePicker();
                for (var type in vm.uploadedFiles['transaction']) {
                    if (vm.uploadedFiles['transaction'][type]) {
                        vm.params[type].scope.cancel();
                    }
                };
            }

            vm.init();

            $scope.$apply();
        }

        vm.clearUploadedFiles = function(entity) {
            for (var type in vm.uploadedFiles[entity]) {
                vm.uploadedFiles[entity][type] = '';
            }; 
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