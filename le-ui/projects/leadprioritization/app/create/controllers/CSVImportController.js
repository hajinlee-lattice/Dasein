angular
.module('lp.create.import', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'lp.create.import.job',
    'lp.create.import.report',
    '720kb.tooltips'
])
.controller('csvImportController', function($scope, $state, $q, ResourceUtility, StringUtility, ImportService, ImportStore) {
    var vm = this;

    angular.extend(vm, {
        importErrorMsg: 'hgiu',
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
            timestamp = year +''+ month +''+ day +'-'+ hours +''+ minutes,
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
    }

    vm.fileDone = function(result) {
        vm.uploaded = true;
        vm.fileName = result.Result.name;
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