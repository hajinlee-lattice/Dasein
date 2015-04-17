angular.module('mainApp.models.controllers.MultipleModelSetupController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.models.services.ModelService',
    'mainApp.models.modals.AddSegmentModal'
])

.controller('MultipleModelSetupController', function ($scope, BrowserStorageUtility, ResourceUtility, ModelService, AddSegmentModal) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.loading = true;
    $scope.segments = [];
    if (BrowserStorageUtility.getClientSession() == null) { 
        return; 
    }
    
    $scope.models = [{
        Id: "FAKE_MODEL",
        DisplayName: "Select"
    }];
    
    function sortByPriority (a, b) {
        if (a.Priority < b.Priority) {
            return -1;
        }
        if (a.Priority > b.Priority) {
            return 1;
        }
        // a must be equal to b
        return 0;
    }
    
    ModelService.GetAllModels(true).then(function(result) {
        // Get model list, but it may be empty
        if (result != null && result.success === true) {
            $scope.models = $scope.models.concat(result.resultObj);
        }
        
        ModelService.GetAllSegments($scope.models).then(function(result) {
            $scope.loading = false;
            if (result != null && result.success === true) {
                for (var i=0;i<result.resultObj.length;i++){
                    //Need this to verify changes against the model in the list
                    result.resultObj[i].NewModelId = result.resultObj[i].ModelId;
                }
                $scope.segments = result.resultObj.sort(sortByPriority);
            } else {
                // Need to handle error case
            }
        });
    });
    
    $scope.addNewSegmentClicked = function () {
        AddSegmentModal.show($scope.segments, $scope.models, function (segment) {
            if (segment != null) {
                segment.NewModelId = segment.ModelId;
                $scope.segments.push(segment);
            }
        });
    };
    
    $scope.deleteSegmentClicked = function (segment) {
        if (segment == null) {
            return;
        }
        ModelService.DeleteSegment(segment.Name).then(function(result) {
            if (result != null && result.success === true) {
                for (var i=0;i<$scope.segments.length;i++) {
                    if (segment.Name == $scope.segments[i].Name) {
                        $scope.segments.splice(i, 1);
                        break;
                    }
                }
            } else {
                // Need to handle error case
            }
        });
        
    };
    
    function updateSegment (segment, successCallback) {
        ModelService.UpdateSegment(segment).then(function(result) {
            if (result != null && result.success === true) {
                if (successCallback) {
                    successCallback();
                }
            } else {
                // Need to handle error case
            }
        });
    }
    
    $scope.modelChanged = function (segment) {
        if (segment.ModelId == segment.NewModelId) {
            return;
        }
        
        var model = null;
        for (var i=0;i<$scope.models.length;i++){
            if (segment.NewModelId == $scope.models[i].Id) {
                model = $scope.models[i];
                break;
            }
        }
        
        if (model == null) {
            return;
        }
        
        var secondSegment = null;
        if (segment.NewModelId == "FAKE_MODEL") {
            segment.ModelId = null;
            segment.ModelName = null;
        } else {
            segment.ModelId = model.Id;
            segment.ModelName = model.DisplayName;
            
            // now we need to figure out if that model was associated to another segment and remove it
            for (var x=0;x<$scope.segments.length;x++) {
                if ($scope.segments[x].ModelId == model.Id) {
                    secondSegment = $scope.segments[x];
                    break;
                }
            }
            
            if (secondSegment != null) {
                secondSegment.ModelId = null;
                secondSegment.ModelName = null;
            }
        }
        
        // Save updated segments here
        if (secondSegment) {
            updateSegment(secondSegment, function () {
                updateSegment(segment);
            });
        } else {
            updateSegment(segment);
        }
    };
    
    $scope.showDecreasePriority = function (segment) {
        var toReturn = false;
        if (segment == null) {
            return toReturn;
        }
        
        for (var i=0;i<$scope.segments.length;i++) {
            if (segment.Name != $scope.segments[i].Name && segment.Priority < $scope.segments[i].Priority) {
                toReturn = true;
                break;
            }
        }
        return toReturn;
    };
    
    $scope.showIncreasePriority = function (segment) {
        var toReturn = false;
        if (segment == null) {
            return toReturn;
        }
        
        for (var i=0;i<$scope.segments.length;i++) {
            if (segment.Name != $scope.segments[i].Name && segment.Priority > $scope.segments[i].Priority) {
                toReturn = true;
                break;
            }
        }
        return toReturn;
    };
    
    $scope.decreasePriorityClicked = function ($event, segment) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        if (segment == null) {
            return;
        }
        
        var nextLowest = null;
        for (var i=0;i<$scope.segments.length;i++) {
            if (segment.Name == $scope.segments[i].Name) {
                var next = i + 1;
                nextLowest = $scope.segments[next];
                break;
            }
        }
        
        var currentPriority = segment.Priority;
        if (nextLowest) {
            segment.Priority = nextLowest.Priority;
            nextLowest.Priority = currentPriority;
            $scope.segments = $scope.segments.sort(sortByPriority);
            updateSegment(segment, function () {
                updateSegment(nextLowest, function () {
                    $scope.segments = $scope.segments.sort(sortByPriority);
                });
            });
        }
        
    };
    
    $scope.increasePriorityClicked = function ($event, segment) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        if (segment == null) {
            return;
        }
        
        var nextHighest = null;
        for (var i=0;i<$scope.segments.length;i++) {
            if (segment.Name == $scope.segments[i].Name) {
                var previous = i - 1;
                nextHighest = $scope.segments[previous];
                break;
            }
        }
        
        var currentPriority = segment.Priority;
        if (nextHighest) {
            segment.Priority = nextHighest.Priority;
            nextHighest.Priority = currentPriority;
            $scope.segments = $scope.segments.sort(sortByPriority);
            updateSegment(segment, function () {
                updateSegment(nextHighest, function () {
                    $scope.segments = $scope.segments.sort(sortByPriority);
                });
            });
        }
    };
});