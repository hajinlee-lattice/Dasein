angular.module('lp.ratingsengine.wizard.training')
.controller('RatingsEngineCustomEventTraining', function (
    $q, $scope, $stateParams, RatingsEngineStore, ResourceUtility, ImportStore) {
    var vm = this;
    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        params: {
            infoTemplate: "<div class='row divider'><div class='twelve columns'><h4>What is a Training File?</h4><p>A training set is a CSV file with records of your historical successes. It is used to build your ideal customer profile by leveraging the Lattice Predictive Insights platform. Ideal training set should have at least 7,000 accounts, 150 success events and a conversion rate of less than 10%.</p></div></div><div class='row'><div class='twelve columns'><h4>Account Model:</h4><p>Upload a CSV file with accounts</p><p>Required: Id (any unique value for each record), Website (domain of company website), Event (1 for success, 0 otherwise)</p></div></div>",
            compressed: true,
            importError: false,
            importErrorMsg: '',
            tooltipConfiguration: {
                tooltipSide: 'bottom',
                whiteFont: true
            }
        },
        uploaded: false,
        goState: null,
        next: false,
        oneRecordPerAccount: false,
        includePersonalEmailDomains: false,
        useCuratedAttributes: false,
        modelTrainingOptions: RatingsEngineStore.getModelTrainingOptions()
    });

    vm.init = function () {
        RatingsEngineStore.setValidation("training", false);

    }

    vm.fileLoad = function(headers) {
        var columns = headers.split(','),
            nonDuplicatedColumns = [],
            duplicatedColumns = [],
            schemaSuggestion;

        vm.params.importError = false;
        vm.showImportError = false;

        vm.params.infoTemplate = "<p>Please prepare a CSV file with the data you wish to import, using the sample CSV file above as a guide.</p><p>You will be asked to map your fields to the Lattice system, so you may want to keep the uploaded file handy for the next few steps.</p>";

        if (columns.length > 0) {
            for (var i = 0; i < columns.length; i++) {
                if (nonDuplicatedColumns.indexOf(columns[i]) < 0) {
                    nonDuplicatedColumns.push(columns[i]);
                } else {
                    duplicatedColumns.push(columns[i]);
                }
            }
            if (duplicatedColumns.length != 0) {
                vm.showImportError = true;
                vm.importErrorMsg = "Duplicate column(s) detected: '[" + duplicatedColumns + "]'";
                vm.params.importError = true;
            }

            var hasWebsite = columns.indexOf('Website') != -1 || columns.indexOf('"Website"') != -1,
                hasEmail = columns.indexOf('Email') != -1 || columns.indexOf('"Email"') != -1;

            // do stuff
        }
    }

    vm.fileSelect = function(fileName) {
        setTimeout(function() {
            vm.uploaded = false;
        }, 25);
    }

    vm.fileDone = function(result) {
        vm.uploaded = true;
        if (result.Result) {
            vm.fileName = result.Result.name;
            RatingsEngineStore.setCSVFileName(vm.fileName);
            RatingsEngineStore.setDisplayFileName(result.Result.display_name);
            vm.next = vm.goState;
            RatingsEngineStore.setValidation("training", true);
        }
    }
    
    vm.fileCancel = function() {
        var xhr = ImportStore.Get('cancelXHR', true);
        
        if (xhr) {
            xhr.abort();
        }
    }

    vm.setTrainingOptions = function(trainingOption) {
            var current = RatingsEngineStore.modelTrainingOptions[trainingOption];
            switch (trainingOption) {
                case 'deduplicationType':
                    RatingsEngineStore.modelTrainingOptions[trainingOption] = current == 'ONELEADPERDOMAIN' ? 'MULTIPLELEADSPERDOMAIN' : 'ONELEADPERDOMAIN';
                    return;
                case 'excludePublicDomains':
                    RatingsEngineStore.modelTrainingOptions[trainingOption] = !current;
                    return;
                case 'transformationGroup':
                    RatingsEngineStore.modelTrainingOptions[trainingOption] = current == 'none' ? null : 'none';
                    return;
            }
            

    }

    vm.init();
});