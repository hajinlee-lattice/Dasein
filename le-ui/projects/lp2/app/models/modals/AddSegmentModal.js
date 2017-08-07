angular.module('mainApp.models.modals.AddSegmentModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService'
])

.service('AddSegmentService', function (StringUtility) {
    
    this.GetLowestPriorityAvailable = function (segments) {
        if (segments == null || segments.length === 0) {
            return 1;
        }
        var lowestPriority = null;
        for (var i=0;i<segments.length;i++) {
            if (lowestPriority == null) {
                lowestPriority = segments[i].Priority + 1;
            } else if (segments[i].Priority <= lowestPriority) {
                lowestPriority = segments[i].Priority + 1;
            }
            
        }
        return lowestPriority;
    };
    
    this.ValidateSegmentName = function (name, segments) {
        if (StringUtility.IsEmptyString(name)) {
            return false;
        }
        
        if (segments == null || segments.length === 0) {
            return true;
        }
        
        for (var i=0;i<segments.length;i++) {
            var segmentName = segments[i].Name.trim();
            if (segmentName.toLowerCase() == name.toLowerCase()) {
                return false;
            }
        }
        
        return true;
    };
})

.service('AddSegmentModal', function ($compile, $rootScope, $http) {
    this.show = function (segments, models, successCallback) {
        $http.get('./app/models/views/AddSegmentView.html').success(function (html) {
            
            var scope = $rootScope.$new();
            scope.segments = segments;
            scope.models = models;
            scope.successCallback = successCallback;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);
            
            var options = {
                backdrop: "static"
            };
            modalElement.modal(options);
            modalElement.modal('show');
            
            // Remove the created HTML from the DOM
            modalElement.on('hidden.bs.modal', function (evt) {
                modalElement.empty();
            });
        });
    };
})

.controller('AddSegmentController', function ($scope, $rootScope, ResourceUtility, ModelService, AddSegmentService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.saveInProgress = false;
    $scope.showAddSegmentError = false;
    $scope.addSegmentErrorMessage = "";
    
    var segments = $scope.segments;
    $scope.filteredModels = $scope.models;
    
    // Add the empty model so they don't have to choose one
    var hasEmptyModel = false;
    for (var i=0;i<$scope.models.length;i++) {
        if ($scope.models[i].Id == "FAKE_MODEL") {
            hasEmptyModel = true;
            break;
        }
    }
    if (!hasEmptyModel) {
        var emptyModel = {
            Id: "FAKE_MODEL",
            DisplayName: "Select"
        };
        $scope.models.unshift(emptyModel);
    }
    
    $scope.newSegment = {
        Name: null,
        Priority: AddSegmentService.GetLowestPriorityAvailable(segments),
        ModelId: null,
        ModelName: null
    };

    $scope.addSegmentClick = function () {
        $scope.addSegmentErrorMessage = "";
        $scope.showAddSegmentError = false;
        var isValid = AddSegmentService.ValidateSegmentName($scope.newSegment.Name, segments);
        if (isValid) {
            $scope.saveInProgress = true;
            
            var modelId = $(".js-model-select").val();
            var secondSegment = null;
            if (modelId != "FAKE_MODEL") {
                $scope.newSegment.ModelId = modelId;
                $scope.newSegment.ModelName = $(".js-model-select option:selected").text();
                
                for (var i=0;i<segments.length;i++) {
                    if (segments[i].ModelId == modelId) {
                        secondSegment = $scope.segments[i];
                        break;
                    }
                }
            }
            
            if (secondSegment != null) {
                secondSegment.ModelId = "FAKE_MODEL";
                secondSegment.ModelName = "";
                ModelService.UpdateSegment(secondSegment).then(function(result) {
                    if (result != null && result.success === true) {
                        addSegment();
                    } else {
                       $scope.saveInProgress = false;
                       $scope.showAddSegmentError = true;
                       $scope.addSegmentErrorMessage = ResourceUtility.getString("ACTIVATE_MODEL_UPDATE_SEGMENT_ERROR", [secondSegment.Name]);
                    }
                });
            } else {
                addSegment();
            }
        } else {
            $scope.addSegmentErrorMessage = ResourceUtility.getString('ACTIVATE_MODEL_ADD_SEGMENT_NAME_ERROR');
            $scope.showAddSegmentError = true;
        }
    };
    
    function addSegment () {
        ModelService.AddSegment($scope.newSegment).then(function(result) {
            $scope.saveInProgress = false;
            if (result && result.success === true) {
                $("#modalContainer").modal('hide');
                if ($scope.successCallback) {
                    $scope.successCallback($scope.newSegment);
                }
            } else {
                $scope.addSegmentErrorMessage = result.resultErrors;
                $scope.showAddSegmentError = true;
            }
        });
    }
    
    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };
    
});