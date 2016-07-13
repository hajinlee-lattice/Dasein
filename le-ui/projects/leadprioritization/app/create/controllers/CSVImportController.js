angular
.module('lp.create.import', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'lp.create.import.job',
    'lp.create.import.report',
    '720kb.tooltips'
])
.controller('csvImportController', function(
    $scope, $state, $q, ResourceUtility, StringUtility, ImportService, 
    ImportStore, FeatureFlagService
) {
    var vm = this;

    FeatureFlagService.GetAllFlags().then(function(result) {
        var flags = FeatureFlagService.Flags();
        
        vm.showPivotMapping = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
    });

    angular.extend(vm, {
        importErrorMsg: '',
        accountLeadCheck: '',
        modelDisplayName: '',
        modelDescription: '',
        uploaded: false,
        showTypeDefault: false,
        showNameDefault: false,
        showImportError: false,
        showImportSuccess: false,
        ResourceUtility: ResourceUtility,
        params: {
            infoTemplate: "<h4>CSV File</h4><p>Creating a CSV file with one row per lead with the fields which you want to train the prediction model with. For best results, there should be at least 50,000 leads, 500 of which should indicate success, and the conversion rate should be between 1% and 10%.</p><h4 class='divider'>Fields For Account Model</h4><p>Required fields are: Id, Website and Event.</p><p>Additional fields are: CompanyName, City, State, Country, PostalCode, Industry, AnnualRevenue, NumberOfEmployees, CreatedDate, LastModifiedDate, YearStarted, PhoneNumber</p><h4 class='divider'>Fields For Lead Model</h4><p>Required fields are: Id, Email, Event.</p><p>Addtional fields are: CompanyName, City, State, Country, PostalCode, CreatedDate, LastModifiedDate, FirstName, LastName, Title, LeadSource, IsClosed, PhoneNumber, AnnualRevenue, NumberOfEmployees, Industry</p>",
            compressed: true
        },
        pivotParams: {
            infoTemplate: "<h4>Pivot Mapping File</h4><p>Choose a Pivot Mapping File</p>",
            defaultMessage: "Example: pivot-mapping.txt",
            compressed: false,
            metadataFile: true
        }
    });

    vm.fileLoad = function(headers) {
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
    }
    
    vm.fileSelect = function(fileName) {
        setTimeout(function() {
            vm.uploaded = false;

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
                schema = !vm.accountLeadCheck 
                    ? '' : (vm.accountLeadCheck == "SalesforceLead") 
                        ? 'lead_' : 'account_',
                timestamp = schema +''+ year +''+ month +''+ day +'-'+ hours +''+ minutes,
                displayName = fileName.replace('.csv',''),
                displayName = displayName.substr(0, 50 - (timestamp.length + 1));

            if ((vm.modelDisplayName || '').indexOf(displayName) < 0) {
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

            var timestamp = new Date().getTime();
                artifactName = vm.artifactName = vm.stripExt(fileName),
                moduleName = vm.moduleName = artifactName + '_' + timestamp;
        }, 25);
    }

    vm.fileDone = function(result) {
        vm.uploaded = true;

        if (result.Result) {
            vm.fileName = result.Result.name;
        }
    }
    
    vm.fileCancel = function() {
        if (vm.showTypeDefault) {
            vm.showTypeDefault = false;
            vm.accountLeadCheck = '';
        }

        if (vm.showNameDefault) {
            vm.showNameDefault = false;
            vm.modelDisplayName = '';
        }

        var xhr = ImportStore.Get('cancelXHR', true);
        
        if (xhr) {
            xhr.abort();
        }
    }

    vm.pivotSelect = function(fileName) {
        var artifactName = vm.artifactName = vm.stripExt(fileName),
            pivotFile = vm.pivotFileName = fileName,
            endpoint = '/pls/metadatauploads/modules/',
            moduleName = vm.moduleName;

        vm.pivotUploaded = false;
        vm.pivotParams.url = endpoint + moduleName + '/pivotmappings?artifactName=' + artifactName;
        
        return vm.pivotParams;
    }

    vm.pivotDone = function() {
        vm.pivotUploaded = true;
    }

    vm.pivotCancel = function() { }

    vm.stripExt = function(fileName) {
        var fnSplit = (fileName || '').split('.');

        if (fnSplit.length > 1) {
            fnSplit.pop();
        }

        return fnSplit.join('.');
    }

    vm.changeType = function() {
        vm.showTypeDefault = false;
    }
    
    vm.changeName = function() {
        vm.showNameDefault = false;
    }
    
    vm.clickUpload = function() {
        vm.showImportError = false;
        vm.importErrorMsg = "";
    }
    
    vm.clickNext = function(fileName) {
        var fileName = fileName || vm.fileName,
            metaData = vm.metadata = vm.metadata || {},
            displayName = vm.modelDisplayName,
            modelName = StringUtility.SubstituteAllSpecialCharsWithDashes(displayName),
            schemaInterpretation = vm.accountLeadCheck;

        metaData.name = fileName;
        metaData.modelName = modelName;
        metaData.displayName = displayName;
        metaData.description = vm.modelDescription;
        metaData.schemaInterpretation = schemaInterpretation;

        if (vm.pivotUploaded) {
            metaData.moduleName = vm.moduleName;
            metaData.pivotFileName = vm.stripExt(vm.pivotFileName) + '.csv';
        }

        ImportStore.Set(fileName, metaData);

        setTimeout(function() {
            $state.go('home.models.import.columns', { csvFileName: fileName });
        }, 1);
    }
    
    vm.keyupTextArea = function(event) {
        var target = event.target;
        target.style.height = 0;
        target.style.height = target.scrollHeight + 'px';
    }
});