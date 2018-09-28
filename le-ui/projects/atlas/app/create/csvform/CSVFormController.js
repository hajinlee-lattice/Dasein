angular
.module('lp.create.import', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'lp.create.import.job',
    'lp.create.import.report',
    'mainApp.setup.modals.FieldMappingSettingsModal',
    '720kb.tooltips'
])
.controller('csvImportController', function(
    $scope, $state, $q, ResourceUtility, StringUtility, ImportService, FieldMappingSettingsModal,
    ImportStore, FeatureFlagService, CancelJobModal, ServiceErrorUtility, Banner
) {
    var vm = this;

    FeatureFlagService.GetAllFlags().then(function(result) {
        var flags = FeatureFlagService.Flags();
        vm.showPivotMapping = FeatureFlagService.FlagIsEnabled(flags.ALLOW_PIVOT_FILE);
    });

    ImportStore.ResetAdvancedSettings();

    angular.extend(vm, {
        importErrorMsg: '',
        accountLeadCheck: '',
        modelDisplayName: '',
        modelDescription: '',
        notesContent: '',
        uploaded: false,
        showTypeDefault: false,
        showNameDefault: false,
        showImportError: false,
        showImportSuccess: false,
        oneLeadPerDomain: ImportStore.GetAdvancedSetting('oneLeadPerDomain'),
        includePersonalEmailDomains: ImportStore.GetAdvancedSetting('includePersonalEmailDomains'),
        useLatticeAttributes: ImportStore.GetAdvancedSetting('useLatticeAttributes'),
        enableTransformations: ImportStore.GetAdvancedSetting('enableTransformations'),
        ResourceUtility: ResourceUtility,
        params: {
            infoTemplate: "<div class='row divider'><div class='twelve columns'><h4>What is a Training File?</h4><p class='template-description'>A training set is a CSV file with records of your historical successes. It is used to build your ideal customer profile by leveraging the Lattice Predictive Insights platform. Ideal training set should have at least 7,000 accounts, 150 success events and a conversion rate of less than 10%.</p></div></div><hr><div class='row'><div class='six columns'><h4>Account Model:</h4><p>Upload a CSV file with accounts</p><p>Required: Id (any unique value for each record), Website (domain of company website), Event (1 for success, 0 otherwise)</p><p>Optional fields: Additional internal attributes about the accounts you would like to use as predictive attributes.</p></div><div class='six columns'><h4>Lead Model:</h4><p>Upload a CSV file with leads</p><p>Required: Id (any unique value for each record), Email, Event (1 for success, 0 otherwise)</p><p>Optional: Lead engagement data can be used as predictive attributes. Below are supported attributes:<ul><li>Marketo (4 week counts): Email Bounces (Soft), Email Clicks, Email Opens, Email Unsubscribes, Form Fills, Web-Link Clicks, Webpage Visits, Interesting Moments</li><li>Eloqua (4 week counts): Email Open, Email Send, Email Click Though, Email Subscribe, Email Unsubscribe, Form Submit, Web Visit, Campaign Membership, External Activity</li></ul></p></div></div>",
            compressed: true,
            importError: false,
            importErrorMsg: '',
            tooltipConfiguration: {
                tooltipSide: 'bottom',
                whiteFont: true
            }
        },
        pivotParams: {
            infoTemplate: "<h4>Pivot Mapping File</h4><p>Choose a Pivot Mapping File</p>",
            defaultMessage: "Example: pivot-mapping.txt",
            compressed: false,
            metadataFile: true
        }
    });

    vm.advancedSettingsClicked = function() {
        FieldMappingSettingsModal.showForModelCreation(
            ImportStore.GetAdvancedSetting('oneLeadPerDomain'),
            ImportStore.GetAdvancedSetting('includePersonalEmailDomains'), 
            ImportStore.GetAdvancedSetting('useLatticeAttributes'),
            ImportStore.GetAdvancedSetting('enableTransformations'),
            vm.accountLeadCheck
        );
    };

    vm.fileLoad = function(headers) {
        var columns = headers.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/),
            columnLimit = 200,
            nonDuplicatedColumns = [],
            duplicatedColumns = [],
            schemaSuggestion;
        
        vm.params.importError = false;
        vm.showImportError = false;

        if (columns.length > 0) {

            if(columns.length > columnLimit) {
                var columnErrorMsg = 'We are unable to build your model because your file is too large.  Your file has ' + columns.length + ' columns.  Please reduce the numer of columns to below ' + columnLimit + '.';
                vm.showImportError = false;
                vm.params.importError = true;
                Banner.error({message: columnErrorMsg});
            }

            for (var i = 0; i < columns.length; i++) {
                if (nonDuplicatedColumns.indexOf(columns[i]) < 0) {
                    nonDuplicatedColumns.push(columns[i]);
                } else {
                    duplicatedColumns.push(columns[i]);
                }
            }
            if (duplicatedColumns.length != 0) {
                vm.showImportError = false;
                vm.importErrorMsg = "Duplicate column(s) detected: '[" + duplicatedColumns + "]'";
                vm.params.importError = true;
                Banner.error({message: vm.importErrorMsg});
            }

            var hasWebsite = columns.indexOf('Website') != -1 || columns.indexOf('"Website"') != -1,
                hasEmail = columns.indexOf('Email') != -1 || columns.indexOf('"Email"') != -1;

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

            var timestamp = new Date().getTime(),
                artifactName = vm.artifactName = vm.stripExt(vm.sanitize(fileName)),
                moduleName = vm.moduleName = artifactName + '_' + timestamp;

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

            var modelDisplayNameEl = $('#modelDisplayName');
            modelDisplayNameEl.focus();

            setTimeout(function() {
                modelDisplayNameEl.select();
            }, 1);

            modelDisplayNameEl
                .parent('div.form-group')
                .removeClass('is-pristine');
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
        var fileName = vm.sanitize(fileName),
            artifactName = vm.artifactName = vm.stripExt(fileName),
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

    vm.pivotCancel = function() {
        vm.pivotUploaded = false;
    }

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
            notesContent = vm.notesContent,
            modelName = StringUtility.SubstituteAllSpecialCharsWithDashes(displayName),
            schemaInterpretation = vm.accountLeadCheck;

        metaData.name = fileName;
        metaData.modelName = modelName;
        metaData.displayName = displayName;
        metaData.notesContent = notesContent;
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

    vm.cancelJob = function($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        CancelJobModal.show(null, {sref:'home.models'});
    }

    vm.sanitize = function(fileName) {
        return fileName.replace(/[^A-Za-z0-9_\.]/g,'_');
    }
});