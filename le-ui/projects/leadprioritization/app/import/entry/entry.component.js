angular.module('lp.import.entry', [
    'lp.import.entry.accounts',
    'lp.import.entry.contacts',
    'lp.import.entry.productpurchases',
    'lp.import.entry.productbundles',
    'lp.import.entry.producthierarchy'
])
.controller('ImportEntry', function(
    $state, $stateParams, $scope, FeatureFlagService, ResourceUtility, ImportWizardStore, ImportStore, AuthorizationUtility
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
        next: false,
        showProductBundleImport: false,
        showProductHierarchyImport: false,
        showProductPurchaseImport: false
    });

    vm.init = function() {
        ImportWizardStore.clear();
        vm.setFeatureFlagPermissions();
        vm.changingEntity = false;    
        var state = $state.current.name;
        switch (state) {
            case 'home.import.entry.accounts': vm.changeEntityType('Account', 'accounts'); break;
            case 'home.import.entry.contacts': vm.changeEntityType('Contact', 'contacts'); break;
            case 'home.import.entry.product_purchases': vm.changeEntityType('Transaction', 'product_purchases'); break;
            case 'home.import.entry.product_bundles': vm.changeEntityType('Product', 'product_bundles', 'BundleSchema'); break;
            case 'home.import.entry.product_hierarchy': vm.changeEntityType('Product', 'product_hierarchy', 'HierarchySchema'); break;
        }
    }

    vm.getDefaultMessage = function()  {
        return "your-" + vm.goState.replace('_','-') + ".csv";
    }

    vm.changeEntityType = function(type, goState, feedType) {
        vm.goState = goState || type.toLowerCase();
        ImportWizardStore.setEntityType(type);
        ImportWizardStore.setFeedType(feedType || null);
        if(vm.params.scope){
            vm.params.scope.cancel();
        }
        vm.next = false;
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
        $state.go('home.import.data.' + vm.goState + '.ids');
    }

    vm.setFeatureFlagPermissions = function() {
        var featureFlags = {};
        featureFlags[flags.VDB_MIGRATION] = false;
        featureFlags[flags.ENABLE_FILE_IMPORT] = true;

        vm.showProductPurchaseImport = AuthorizationUtility.checkFeatureFlags(featureFlags) && FeatureFlagService.FlagIsEnabled(flags.ENABLE_PRODUCT_PURCHASE_IMPORT);
        vm.showProductBundleImport = AuthorizationUtility.checkFeatureFlags(featureFlags) && FeatureFlagService.FlagIsEnabled(flags.ENABLE_PRODUCT_BUNDLE_IMPORT);
        vm.showProductHierarchyImport = AuthorizationUtility.checkFeatureFlags(featureFlags) && FeatureFlagService.FlagIsEnabled(flags.ENABLE_PRODUCT_HIERARCHY_IMPORT);
    }

    vm.init();
});