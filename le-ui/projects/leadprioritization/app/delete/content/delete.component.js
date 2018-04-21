angular.module('lp.delete.entry', [])
.component('deleteEntry', {
    templateUrl: 'app/delete/content/delete.component.html',
    controller: function( 
        $state, $stateParams, $scope, DeleteDataService, ImportStore, ResourceUtility, ModalStore) 
    { 
        var vm = this,
            resolve = $scope.$parent.$resolve,
            EntitiesCount = resolve.EntitiesCount;

        angular.extend(vm, {
            uploaded: false,
            showFromTime: true,
            showToTime: true,
            showTimeFrame: false,
            showSuccessMsg: false,
            showWarningMsg: true,
            enableDelete: true,
            ResourceUtility: ResourceUtility,
            currentTab: 'account',
            deleteWarningMsg: 'Once the delete action is submitted, it can’t be undone.',
            startTime: '',
            endTime: '',
            counts: {
                'accounts': EntitiesCount.Account,
                'contacts': EntitiesCount.Contact 
            },
            params: {
                infoTemplate: "<div class='row divider'><div class='twelve columns'><h4>What is a Training File?</h4><p>A training set is a CSV file with records of your historical successes. It is used to build your ideal customer profile by leveraging the Lattice Predictive Insights platform. Ideal training set should have at least 7,000 accounts, 150 success events and a conversion rate of less than 10%.</p></div></div><div class='row'><div class='six columns'><h4>Account Model:</h4><p>Upload a CSV file with accounts</p><p>Required: Id (any unique value for each record), Website (domain of company website), Event (1 for success, 0 otherwise)</p><p>Optional fields: Additional internal attributes about the accounts you would like to use as predictive attributes.</p></div><div class='six columns'><h4>Lead Model:</h4><p>Upload a CSV file with leads</p><p>Required: Id (any unique value for each record), Email, Event (1 for success, 0 otherwise)</p><p>Optional: Lead engagement data can be used as predictive attributes. Below are supported attributes:<ul><li>Marketo (4 week counts): Email Bounces (Soft), Email Clicks, Email Opens, Email Unsubscribes, Form Fills, Web-Link Clicks, Webpage Visits, Interesting Moments</li><li>Eloqua (4 week counts): Email Open, Email Send, Email Click Though, Email Subscribe, Email Unsubscribe, Form Submit, Web Visit, Campaign Membership, External Activity</li></ul></p></div></div>",
                compressed: true,
                importError: false,
                importErrorMsg: '',
                url: '/pls/models/uploadfile/uploaddeletefiletemplate',
                schema: 'DeleteAccountTemplate',
                operationType: 'BYUPLOAD_ID'
            },
            periodTimeConf: {
                from: { name: 'from-time', initial: undefined, position: 0, type: 'Time', visible: true },
                to: { name: 'to-time', initial: undefined, position: 1, type: 'Time', visible: true }
            },
            bucketRestriction: {
               "attr":"delete",
            },
            cleanupOperationType: {
                'account': '',
                'contact': '',
                'transaction': ''
            },
            isValid: {
                'account': false,
                'contact': false,
                'transaction': false
            },
            uploadOperations: ['BYUPLOAD_ID', 'BYUPLOAD_ACPD', 'BYUPLOAD_MINDATEANDACCOUNT'],
            uploadedFiles: {
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
            }
        });

        vm.init = function() {

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

            vm.isValid[vm.currentTab] = ($scope.delete_form['from-time'].$valid && $scope.delete_form['to-time'].$valid);

        }

        vm.changeEntityType = function(type, goState) {
            vm.currentTab = goState || type.toLowerCase();
            
        }

        vm.fileLoad = function(headers) {
            // console.log('headers', headers);
        }

        vm.fileSelect = function(fileName) {
            vm.params.operationType = vm.getCleanupType();
            vm.params.schema = vm.getSchema();

            setTimeout(function() {
                vm.uploaded = false;
            }, 25);
        }

        vm.fileDone = function(result) {
            vm.uploaded = true;

            if (result.Result) {
                vm.fileName = result.Result.name;
                vm.cleanupOperationType[vm.currentTab];
                vm.uploadedFiles[vm.currentTab][vm.cleanupOperationType[vm.currentTab]] = result.Result.name;
                vm.isValid[vm.currentTab] = vm.uploadedFiles[vm.currentTab][vm.cleanupOperationType[vm.currentTab]] != '';
            }
        }
        
        vm.fileCancel = function() {
            var xhr = ImportStore.Get('cancelXHR', true);
            
            if (xhr) {
                xhr.abort();
            }
        }

        vm.setCleanupType = function(option) {
            vm.cleanupOperationType[vm.currentTab] = option;
            if (vm.uploadOperations.indexOf(vm.getCleanupType()) >= 0) {
                vm.isValid[vm.currentTab] = vm.uploadedFiles[vm.currentTab][option]
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

        vm.submitCleanupJob = function() {
            if (vm.uploadOperations.indexOf(vm.getCleanupType()) >= 0) {
                DeleteDataService.cleanupByUpload(vm.fileName, vm.getSchema(), vm.getCleanupType()).then (function(result) {
                    if (result && result.Success) {
                        vm.toggleBannerMsg();
                    }
                });
            } else if (vm.getCleanupType() == 'BYDATERANGE') {
                DeleteDataService.cleanupByDateRange(vm.startTime, vm.endTime, vm.getSchema()).then (function(result) {
                    if (result && result.Success) {
                        vm.toggleBannerMsg();
                    }
                });       
            } else { // ALLDATA
                DeleteDataService.cleanupAllData(vm.getSchema()).then(function(result) {
                    if (result && result.Success) {
                        vm.toggleBannerMsg();
                    }
                });                          
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
                if (vm.modalConfig.dischargeaction === args) {
                    vm.toggleModal();
                } else if (vm.modalConfig.confirmaction === args) {
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

            vm.toggleBannerMsg = function() {
                vm.showSuccessMsg = !vm.showSuccessMsg;
                vm.showWarningMsg = !vm.showWarningMsg;
            }

            $scope.$on("$destroy", function () {
                ModalStore.remove(vm.modalConfig.name);
            });
        }

        vm.initModalWindow();

        vm.init();
    }
});