angular.module('lp.import.entry', [
    'lp.import.entry.accounts',
    'lp.import.entry.accountfields',
    'lp.import.entry.contacts',
    'lp.import.entry.contactfields',
    'lp.import.entry.eloquoa'
])
.controller('ImportEntry', function(
    $state, $stateParams, $scope, FeatureFlagService, ResourceUtility, ImportWizardStore, ImportStore
) {
    var vm = this,
        flags = FeatureFlagService.Flags();

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        params: {
            infoTemplate: "<div class='row divider'><div class='twelve columns'><h4>What is a Training File?</h4><p>A training set is a CSV file with records of your historical successes. It is used to build your ideal customer profile by leveraging the Lattice Predictive Insights platform. Ideal training set should have at least 7,000 accounts, 150 success events and a conversion rate of less than 10%.</p></div></div><div class='row'><div class='six columns'><h4>Account Model:</h4><p>Upload a CSV file with accounts</p><p>Required: Id (any unique value for each record), Website (domain of company website), Event (1 for success, 0 otherwise)</p><p>Optional fields: Additional internal attributes about the accounts you would like to use as predictive attributes.</p></div><div class='six columns'><h4>Lead Model:</h4><p>Upload a CSV file with leads</p><p>Required: Id (any unique value for each record), Email, Event (1 for success, 0 otherwise)</p><p>Optional: Lead engagement data can be used as predictive attributes. Below are supported attributes:<ul><li>Marketo (4 week counts): Email Bounces (Soft), Email Clicks, Email Opens, Email Unsubscribes, Form Fills, Web-Link Clicks, Webpage Visits, Interesting Moments</li><li>Eloqua (4 week counts): Email Open, Email Send, Email Click Though, Email Subscribe, Email Unsubscribe, Form Submit, Web Visit, Campaign Membership, External Activity</li></ul></p></div></div>",
            compressed: true,
            importError: false,
            importErrorMsg: '',
        },
        uploaded: false,
        goState: null,
        next: false
    });

    vm.init = function() {
        var state = $state.current.name;
        switch (state) {
            case 'home.import.entry.accounts': vm.changeEntityType('accounts'); break;
            case 'home.import.entry.accountfields': vm.changeEntityType('accountfields'); break;
            case 'home.import.entry.contacts': vm.changeEntityType('contacts'); break;
            case 'home.import.entry.contactfields': vm.changeEntityType('contactfields'); break;
            case 'home.import.entry.eloquoa': vm.changeEntityType('eloquoa'); break;
        }
    }

    vm.changeEntityType = function(type) {
        vm.goState = type;
        ImportWizardStore.setEntityType(type);
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
            vm.next = vm.goState;
        }
    }
    
    vm.fileCancel = function() {
        var xhr = ImportStore.Get('cancelXHR', true);
        
        if (xhr) {
            xhr.abort();
        }
    }

    vm.click = function() {
        $state.go('home.import.wizard.' + vm.goState + '.one');
    }

    vm.init();
});