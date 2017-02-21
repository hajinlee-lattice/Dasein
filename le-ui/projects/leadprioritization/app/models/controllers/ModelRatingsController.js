angular.module('lp.models.ratings', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.ModelDetailsWidget',
    'mainApp.models.services.ModelService'
])
.controller('ModelRatingsController', function ($scope, $rootScope, $stateParams,
    ResourceUtility, Model, ModelStore, ModelRatingsService) {

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        model: Model,
        data: ModelStore.data,
        chartNotUpdated: true,
        saveInProgress: false,
        showSaveBucketsError: false,
        ResourceUtility: ResourceUtility,
        workingBuckets: []
    });


    vm.init = function() {

        $scope.data = ModelStore.data;
        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });
        $scope.Math = window.Math;

        ModelRatingsService.MostRecentConfiguration(vm.modelId).then(function(result) { 
            vm.currentConfiguration = result;
        });
        ModelRatingsService.GetBucketedScoresSummary(vm.modelId).then(function(result) {  
            var summary = result; 
            vm.summary = summary;
            renderChart(summary);
        });
        
        if(vm.model.EventTableProvenance.SourceSchemaInterpretation === "SalesforceLead"){
            vm.modelType = "Leads";
        } else {
            vm.modelType = "Accounts";
        }
        
    }

    vm.publishConfiguration = function() {
        vm.chartNotUpdated = false;
        ModelRatingsService.CreateABCDBuckets().then(function(result){
            if (result != null && result.success === true) {
                $state.go('home.model.ratings', {}, { reload: true });
            } else {
                vm.saveInProgress = false;
                vm.createBucketsErrorMessage = result;
                vm.showSaveBucketsError = true;
            }
        });
    }

    function renderChart(summary){
        
        var verticalAxis = document.getElementById("verticalAxis");

        // Get tallest bar in set
        vm.largestLiftInSet = Math.max.apply(null, summary.bar_lifts);

        // Set height of chart components based off tallest bar
        vm.relativeHeightOfTallest = Math.round((12*vm.largestLiftInSet) + 10);
        if(vm.relativeHeightOfTallest < 150){
            vm.chartContainerHeight = Math.round((25*vm.largestLiftInSet) + 10);
            vm.barMultiplier = 25;
        } else {
            vm.chartContainerHeight = Math.round((15*vm.largestLiftInSet) + 10);
            vm.barMultiplier = 15;
        }

        // Define height of dugout
        vm.dugoutHeight = vm.chartContainerHeight - 8;
        
        // Create vertical axis based on maxLift
        vm.yAxisNumber = Math.round(vm.largestLiftInSet);
        if(vm.yAxisNumber >= 3 && vm.yAxisNumber <= 10) {
            verticalAxis.classList.add('reduceSmall');
        } else if (vm.yAxisNumber > 10) {
            verticalAxis.classList.add('reduceBig');
        }

        vm.getNumber = function(num) {return new Array(num);}
        vm.axisItemHeight = vm.chartContainerHeight / vm.yAxisNumber;
 
    }

    // setTimeout(function(){ initSliders(); }, 3000);

    function initSliders(){

        var elem = document.getElementsByClassName('slider');

        console.log(elem);

        //* toggle direction (remove first slash)
        var direction = 'left', coord = 'X' /*/
        var direction = 'top', coord = 'Y' //*/
        elem.onmousedown = function (evt) {
          evt.stopPropagation()
          evt.preventDefault()
          evt = evt || window.event
          var start = 0, diff = 0
          if ( evt['page' + coord] ) { start = evt['page' + coord] }
          else if ( evt['client' + coord] ) { start = evt['client' + coord] }

          elem.style.position = 'relative'
          document.body.onmousemove = function (evt) {
            evt.stopPropagation()
            evt.preventDefault()
            evt = evt || window.event
            var end = 0
            if ( evt['page' + coord] ) { end = evt['page' + coord] }
            else if ( evt['client' + coord] ) { end = evt['client' + coord] }

            diff = end - start
            elem.style[direction] = diff + 'px'
          }
          document.body.onmouseup = function () {
            evt.stopPropagation()
            evt.preventDefault()
            // do something with the action here
            // elem has been moved by diff pixels in the X axis
            elem.style.position = 'static'
            document.body.onmousemove = document.body.onmouseup = null
          }
        }

    }

    function adjustColors(){
        console.log("adjust colors");
        adjustWorkingBuckets();
    }
    function adjustWorkingBuckets(){
        console.log("adjust working buckets");
    }

    vm.init();
})
.controller('ModelRatingsHistoryController', function ($scope, $rootScope, $stateParams,
    ResourceUtility, Model, ModelStore, ModelRatingsService, HistoricalABCDBuckets) {

    var vm = this;
    angular.extend(vm, {
        model: Model,
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        data: ModelStore,
        ResourceUtility: ResourceUtility,
        historicalBuckets: HistoricalABCDBuckets
    });

    vm.init = function() {
        $rootScope.$broadcast('model-details', { displayName: Model.ModelDetails.DisplayName });
        $scope.Math = window.Math;

        vm.getModelJobNumber = vm.model.ModelDetails.ModelSummaryProvenanceProperties[5].ModelSummaryProvenanceProperty.value;
        // console.log(vm.model.ModelDetails.ModelSummaryProvenanceProperties[5].ModelSummaryProvenanceProperty);
        console.log($stateParams);

        if(vm.model.EventTableProvenance.SourceSchemaInterpretation === "SalesforceLead"){
            vm.modelType = "Leads";
        } else {
            vm.modelType = "Accounts";
        }

        // Set value for total leads in set
        // This will need to get changed when we're saving configurations
        vm.historyTotalLeads = pluckDeepKey("num_leads", vm.historicalBuckets);

        // Add values for a specific key in object
        function pluckDeepKey(key, obj) {
          if (_.has(obj, key)) {
            return obj[key];
          }
          return _.reduce(_.flatten(_.map(obj, function(v) {
            return _.isObject(v) ? pluckDeepKey(key, v) : [];
          }), false), function(a,b) { return a + b });
        }

    };

    vm.init();

});