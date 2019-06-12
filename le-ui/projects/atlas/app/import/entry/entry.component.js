angular.module('lp.import.entry', [
    'lp.import.entry.accounts',
    'lp.import.entry.contacts',
    'lp.import.entry.productpurchases',
    'lp.import.entry.productbundles',
    'lp.import.entry.producthierarchy'
])
.controller('ImportEntry', function(
    $state, $stateParams, $scope, 
    FeatureFlagService, ResourceUtility, ImportWizardStore, ImportWizardService, ImportStore, AuthorizationUtility, Banner
) {
    var vm = this,
        flags = FeatureFlagService.Flags();
    

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        params: {
            infoTemplate: "<div class='row divider'><div class='twelve columns'><h4>What is a Training File?</h4><p>A training set is a CSV file with records of your historical successes. It is used to build your ideal customer profile by leveraging the Lattice Predictive Insights platform. Ideal training set should have at least 7,000 accounts, 150 success events and a conversion rate of less than 10%.</p></div></div><div class='row'><div class='six columns'><h4>Account Model:</h4><p>Upload a CSV file with accounts</p><p>Required: Id (any unique value for each record), Website (domain of company website), Event (1 for success, 0 otherwise)</p><p>Optional fields: Additional internal attributes about the accounts you would like to use as predictive attributes.</p></div><div class='six columns'><h4>Lead Model:</h4><p>Upload a CSV file with leads</p><p>Required: Id (any unique value for each record), Email, Event (1 for success, 0 otherwise)</p><p>Optional: Lead engagement data can be used as predictive attributes. Below are supported attributes:<ul><li>Marketo (4 week counts): Email Bounces (Soft), Email Clicks, Email Opens, Email Unsubscribes, Form Fills, Web-Link Clicks, Webpage Visits, Interesting Moments</li><li>Eloqua (4 week counts): Email Open, Email Send, Email Click Though, Email Subscribe, Email Unsubscribe, Form Submit, Web Visit, Campaign Membership, External Activity</li></ul></p></div></div>",
            compressed: true,
            importError: false,
            invalidcolumns:[],
            importErrorMsg: '',
            editing: $stateParams.editing,
            importOnly: $stateParams.importOnly
        },
        uploaded: false,
        goState: null,
        next: false,
        showButtonSpinner: false,
        showProductBundleImport: false,
        showProductHierarchyImport: false,
        showProductPurchaseImport: false
    });

    vm.init = function() {
        const templateAction = $stateParams.action ? $stateParams.action : ImportWizardStore.getTemplateAction();

        ImportWizardStore.setTemplateAction(templateAction);

        let flowType = "";
        switch (templateAction) {
            case "create-template": {
                flowType = "Create";
                break;
            }
            case "edit-template": {
                flowType = "Edit";
                break;
            }
            case "import-data": {
                flowType = "Import";
                break;
            }
        }
        vm.formType = flowType;
        vm.nextButtonText = vm.formType == 'Import' ? 'Import File' : 'Next, Field Mappings';

        vm.displayType = $stateParams.type ? $stateParams.type : ImportWizardStore.getDisplayType();
        ImportWizardStore.setDisplayType(vm.displayType);        

        ImportWizardStore.clear();
        //ImportWizardStore.setThirdpartyidFields([], []); // we don't clear all the stores but we should clear thirdparty ids

        if ($stateParams.data){
            ImportWizardStore.setPostBody($stateParams.data);
            ImportWizardStore.setFeedType($stateParams.data.FeedType || null);
        }
        
        vm.setFeatureFlagPermissions();
        vm.changingEntity = false;    
        var state = $state.current.name;
        switch (state) {
            case 'home.import.entry.accounts': 
                vm.changeEntityType('Account', 'accounts');
                break;
            case 'home.import.entry.contacts': 
                vm.changeEntityType('Contact', 'contacts'); 
                break;
            case 'home.import.entry.productpurchases': 
                vm.changeEntityType('Transaction', 'productpurchases'); 
                break;
            case 'home.import.entry.productbundles': 
                vm.changeEntityType('Product', 'productbundles', 'BundleSchema'); 
                break;
            case 'home.import.entry.producthierarchy': 
                vm.changeEntityType('Product', 'producthierarchy', 'HierarchySchema'); 
                break;
        }
        vm.next = false;
    }

    vm.getDefaultMessage = function()  {
        return "your-" + vm.goState + ".csv";
    }

    vm.changeEntityType = function(type, goState) {
        vm.goState = goState || type.toLowerCase();
        ImportWizardStore.setEntityType(type);
        if(vm.params.scope){
            vm.params.scope.cancel();
        }
    }

    function getInvalidColumns(columns){
        var invalid = columns.filter(function(element){
            if(element.includes('^/')){
                return true;
            }
        });
        return invalid;
    }

    vm.closeWarning = function(){
        vm.next = false;
        vm.invalidcolumns = [];
        if(vm.params.scope){
            vm.params.scope.cancel();
        }
    }

    vm.fileLoad = function(headers) {
        vm.next = false;
        vm.invalidcolumns = [];
        var columns = headers.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/),
            nonDuplicatedColumns = [],
            duplicatedColumns = [],
            schemaSuggestion;

        vm.params.importError = false;
    
        vm.params.infoTemplate = "<p>Please prepare a CSV file with the data you wish to import, using the sample CSV file above as a guide.</p><p>You will be asked to map your fields to the Lattice system, so you may want to keep the uploaded file handy for the next few steps.</p>";
        vm.invalidcolumns = getInvalidColumns(columns);
        if (columns.length > 0) {
            for (var i = 0; i < columns.length; i++) {
                if (nonDuplicatedColumns.indexOf(columns[i]) < 0) {
                    nonDuplicatedColumns.push(columns[i]);
                } else {
                    duplicatedColumns.push(columns[i]);
                }
            }
            if (duplicatedColumns.length != 0) {
                vm.params.importError = true;
                Banner.error({message: "Duplicate column(s) detected: '[" + duplicatedColumns + "]'"});
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

        if (result.Result && vm.invalidcolumns.length === 0) {
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
        vm.next = false;
        vm.showButtonSpinner = true;
        if(vm.formType != 'Import'){
            $state.go('home.import.data.' + vm.goState + '.ids');
        } else {
            var fileName = ImportWizardStore.getCsvFileName(),
                importOnly = true,
                autoImportData = ImportWizardStore.getAutoImport(),
                postBody = ImportWizardStore.getTemplateData();

            console.log(fileName, importOnly, autoImportData, postBody);
            ImportWizardService.templateDataIngestion(fileName, importOnly, autoImportData, postBody).then(function(){
                $state.go('home.importtemplates');
            });
        }
    }

    vm.goBack = () => {
        var flags = FeatureFlagService.Flags();
        if(FeatureFlagService.FlagIsEnabled(flags.ENABLE_MULTI_TEMPLATE_IMPORT)){
            $state.go('home.multipletemplates');
        }else{
            $state.go('home.importtemplates');
        }
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