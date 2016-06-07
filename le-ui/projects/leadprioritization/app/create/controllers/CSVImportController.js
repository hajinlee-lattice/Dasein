angular
.module('mainApp.create.csvImport', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.modules.ServiceErrorModule',
    '720kb.tooltips'
])
.controller('csvImportController', function($scope, $state, $q, ResourceUtility, StringUtility, csvImportService, csvImportStore) {
    var vm = this,
        options = {
            importErrorMsg: '',
            accountLeadCheck: '',
            modelDisplayName: '',
            modelDescription: '',
            showTypeDefault: false,
            showNameDefault: false,
            showImportError: false,
            showImportSuccess: false,
            ResourceUtility: ResourceUtility
        };

    angular.extend(vm, options, {
        processHeaders: function(headers) {
            var columns = headers.split(','),
                schemaSuggestion;

            if (columns.length > 0) {
                var hasWebsite = columns.indexOf('Website') != -1,
                    hasEmail = columns.indexOf('Email') != -1;

                if (hasWebsite && !hasEmail) {
                    schemaSuggestion = 'SalesforceAccount';
                } 

                if (!hasWebsite && hasEmail) {
                    schemaSuggestion = 'SalesforceLead';
                }

                if (!vm.accountLeadCheck && schemaSuggestion) {
                    vm.accountLeadCheck = schemaSuggestion;
                    vm.showTypeDefault = true;
                }
            }
        },
        generateModelName: function(fileName) {
            if (vm.modelDisplayName) {
                return;
            }

            var date = new Date(),
                day = date.getDate(),
                year = date.getFullYear(),
                month = (date.getMonth() + 1),
                seconds = date.getSeconds(),
                minutes = date.getMinutes(),
                hours = date.getHours(),
                month = (month < 10 ? '0' + month : month),
                day = (day < 10 ? '0' + day : day),
                minutes = (minutes < 10 ? '0' + minutes : minutes),
                hours = (hours < 10 ? '0' + hours : hours),
                timestamp = year +''+ month +''+ day +'-'+ hours +''+ minutes,
                displayName = fileName.replace('.csv',''),
                displayName = displayName.substr(0, 50 - (timestamp.length + 1));

            if (vm.modelDisplayName && vm.modelDisplayName.indexOf(displayName) < 0) {
                vm.modelDisplayName = displayName + '_' + timestamp;
                vm.showNameDefault = true;
            }

            $('#modelDisplayName').focus();
            
            setTimeout(function() {
                $('#modelDisplayName').select();
            }, 1);

            $('#modelDisplayName')
                .parent('div.form-group')
                .removeClass('is-pristine');
        },
        changeType: function() {
            vm.showTypeDefault = false;
        },
        changeName: function() {
            vm.showNameDefault = false;
        },
        clickUpload: function() {
            vm.showImportError = false;
            vm.importErrorMsg = "";
        },
        clickNext: function(fileName) {
            var metaData = vm.metadata = vm.metadata || {},
                displayName = vm.modelDisplayName,
                modelName = StringUtility.SubstituteAllSpecialCharsWithDashes(displayName);

            metaData.name = fileName;
            metaData.modelName = modelName;
            metaData.displayName = displayName;
            metaData.description = vm.modelDescription;
            csvImportStore.Set(fileName, metaData);

            setTimeout(function() {
                $state.go('home.models.import.columns', { csvFileName: fileName });
            }, 1);
        },
        clickCancel: function() {
            if (vm.showTypeDefault) {
                vm.showTypeDefault = false;
                vm.accountLeadCheck = '';
            }

            if (vm.showNameDefault) {
                vm.showNameDefault = false;
                vm.modelDisplayName = '';
            }

            var xhr = csvImportStore.Get('cancelXHR', true);
            
            if (xhr) {
                xhr.abort();
            }
        },
        keyupTextArea: function(event) {
            var target = event.target;
            target.style.height = 0;
            target.style.height = target.scrollHeight + 'px';
        }
    });
});