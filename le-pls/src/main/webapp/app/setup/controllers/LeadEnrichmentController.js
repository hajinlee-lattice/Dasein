angular.module('mainApp.setup.controllers.LeadEnrichmentController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.services.SessionService',
    'mainApp.setup.modals.LeadEnrichmentAttributesDetailsModel',
    'mainApp.setup.services.LeadEnrichmentService'
])

.controller('LeadEnrichmentController', function ($scope, $rootScope, $http, _, ResourceUtility, BrowserStorageUtility, NavUtility, SessionService, LeadEnrichmentAttributesDetailsModel, LeadEnrichmentService) {
    $scope.ResourceUtility = ResourceUtility;
    if (BrowserStorageUtility.getClientSession() == null) { return; }

    $scope.load = function () {
        initialize();
        LeadEnrichmentService.GetAvariableAttributes().then(function (avariableAttributesData) {
            if (!avariableAttributesData.Success) {
                handleLoadError(avariableAttributesData);
                return;
            }

            $scope.allAvariableAttributes = avariableAttributesData.ResultObj;
            LeadEnrichmentService.GetSavedAttributes().then(function (savedAttributesData) {
                if (!savedAttributesData.Success) {
                    handleLoadError(savedAttributesData);
                    return;
                }

                var availableAttributes = [];
                var savedAttributes = savedAttributesData.ResultObj;
                for (var i = 0; i < $scope.allAvariableAttributes.length; i++) {
                    var attr = $scope.allAvariableAttributes[i];
                    if (getAttribute(savedAttributes, attr.FieldName) == null) {
                        availableAttributes.push(attr);
                    }
                }
                $scope.availableAttributes = sortAttributes(availableAttributes);
                $scope.selectedAttributes = sortAttributes(savedAttributes);
                $scope.loading = false;
            });
        });
    };

    function initialize() {
        $scope.loading = true;
        $scope.showLoadingError = false;
        $scope.saveInProgress = false;
        $scope.showSaveAttributesError = false;
        $scope.showAttributesDetails = false;
        $scope.showAttributesExcessAlert = false;
        $scope.maxSelectedAttributes = 20;
        $scope.attributesToAdd = [];
        $scope.ctrlAttributesToAdd = [];
        $scope.attributesToRemove = [];
        $scope.ctrlAttributesToRemove = [];
        $scope.selectedAttributes = [];
        $scope.leadEnrichment = $('#leadEnrichment');
        $scope.attributeHover = $('#leadEnrichmentAttributeHover');
    }

    function handleLoadError(data) {
        $scope.loadingError = data.ResultErrors;
        $scope.showLoadingError = true;
        $scope.loading = false;
    }

    $scope.load();

    $scope.selectedAttributesLabelMouseEnter = function ($event) {
        if ($scope.selectedAttributes.length >= $scope.maxSelectedAttributes) {
            $scope.tooltip = ResourceUtility.getString('LEAD_ENRICHMENT_MAX_SELECTED_ATTRIBUTES_TOOLTIP',
                    [$scope.maxSelectedAttributes]);
            showAttributeHover($($event.currentTarget), $event);
        }
    };

    $scope.selectedAttributesLabelMouseLeave = function ($event) {
        hideAttributeHover();
    };

    $scope.addAttributeButtonClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($('#addLeadEnrichmentAttribute').hasClass('disabled')) {
            return;
        }
        if ($scope.attributesToAdd.length + $scope.selectedAttributes.length > $scope.maxSelectedAttributes) {
            $scope.attributesExcessAlert = ResourceUtility.getString('LEAD_ENRICHMENT_EXCESS_ATTRIBUTES_ALERT',
                    [$scope.attributesToAdd.length, $scope.maxSelectedAttributes]);
            $scope.showAttributesExcessAlert = true;
            return;
        }

        if ($scope.attributesToAdd.length > 0 && $scope.selectedAttributes.length < $scope.maxSelectedAttributes) {
            $scope.showAttributesExcessAlert = false;
            $('#leadEnrichmentAttributeHover').addClass('hide');
            var obj = removeAttributes($scope.availableAttributes, $scope.attributesToAdd);
            $scope.availableAttributes = sortAttributes(obj.remainingAttributes);
            $scope.selectedAttributes = sortAttributes($scope.selectedAttributes.concat(obj.removedAttributes));
            $scope.attributesToAdd = [];
            $scope.ctrlAttributesToAdd = [];
        }
    };

    $scope.removeAttributeButtonClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.attributesToRemove.length > 0) {
            $scope.showAttributesExcessAlert = false;
            $('#leadEnrichmentAttributeHover').addClass('hide');
            var obj = removeAttributes($scope.selectedAttributes, $scope.attributesToRemove);
            $scope.availableAttributes = sortAttributes($scope.availableAttributes.concat(obj.removedAttributes));
            $scope.selectedAttributes = sortAttributes(obj.remainingAttributes);
            $scope.attributesToRemove = [];
            $scope.ctrlAttributesToRemove = [];
        }
    };

    $scope.attributeItemMouseDown = function ($event, isAvailableAttribute) {
        if ($event != null) {
            $event.preventDefault();
        }

        var currentItem = $($event.currentTarget);
        var fieldName = getFieldName(currentItem);
        currentItem.addClass('selected');
        if ($event.ctrlKey) {
            var currentIndex = currentItem.index();
            var attributes = isAvailableAttribute ? $scope.attributesToAdd : $scope.attributesToRemove;
            if (attributes.indexOf(fieldName) === -1) {
                attributes.push(fieldName);
            }
            var ctrlAttributes = isAvailableAttribute ? $scope.ctrlAttributesToAdd : $scope.ctrlAttributesToRemove;
            if (ctrlAttributes.length === 0) {
                for (var i = 0; i < attributes.length; i++) {
                    ctrlAttributes.push(attributes[i]);
                }
            } else if (ctrlAttributes.indexOf(fieldName) === -1) {
                ctrlAttributes.push(fieldName);
            }
        } else {
            currentItem.siblings().removeClass('selected');
            if (isAvailableAttribute) {
                $scope.attributesToAdd = [fieldName];
                $scope.ctrlAttributesToAdd = [];
            } else {
                $scope.attributesToRemove = [fieldName];
                $scope.ctrlAttributesToRemove = [];
            }
        }

        if ($scope.selectionStartItem == null) {
            $scope.selectionStartItem = currentItem;
            $(document).mouseup(documentMouseUp);
        }
    };

    function documentMouseUp() {
        $scope.selectionStartItem = null;
        $(document).unbind("mouseup", documentMouseUp);
    }

    $scope.attributeItemMouseEnter = function ($event, isAvailableAttribute) {
        if ($event != null) {
            $event.preventDefault();
        }

        var currentItem = $($event.currentTarget);
        if ($scope.selectionStartItem != null) {
            var list = currentItem.parent();
            if (list.attr("id") === $scope.selectionStartItem.parent().attr("id")) {
                var startIndex = $scope.selectionStartItem.index();
                var endIndex = currentItem.index();
                if (startIndex > endIndex) {
                    var temp = endIndex;
                    endIndex = startIndex;
                    startIndex = temp;
                }

                var selectedItems = [];
                var allItems = list.find('li');
                var ctrlAttributes = isAvailableAttribute ? $scope.ctrlAttributesToAdd : $scope.ctrlAttributesToRemove;
                allItems.each(function (index) {
                    var item = $(this);
                    if (index >= startIndex && index <= endIndex) {
                        selectedItems.push(getFieldName(item));
                        item.addClass('selected');
                    } else {
                        var fieldName = getFieldName(item);
                        if (ctrlAttributes.indexOf(fieldName) > -1) {
                            selectedItems.push(fieldName);
                            item.addClass('selected');
                        } else {
                            item.removeClass('selected');
                        }
                    }
                });
                if (isAvailableAttribute) {
                    $scope.attributesToAdd = selectedItems;
                } else {
                    $scope.attributesToRemove = selectedItems;
                }
            }
        } else {
            var attr = getAttribute($scope.allAvariableAttributes, getFieldName(currentItem));
            if (attr != null && attr.Description != null && attr.Description.length > 0) {
                $scope.tooltip = attr.Description;
                showAttributeHover(currentItem, $event);
            }
        }
    };

    $scope.attributeItemMouseLeave = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        hideAttributeHover();
    };

    $scope.saveAttributesButtonClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        LeadEnrichmentAttributesDetailsModel.show($scope);
    };

    $scope.setAttributesDetails = function (value) {
        $scope.showAttributesDetails = value;
    };

    function removeAttributes(attributes, attributesToRemove) {
        var removedAttributes = [];
        for (var i = 0; i < attributesToRemove.length; i++) {
            var attrToRemove = getAttribute(attributes, attributesToRemove[i]);
            if (attrToRemove != null) {
                removedAttributes.push(attrToRemove);
            }
        }
        var remainingAttributes = [];
        for (var j = 0; j < attributes.length; j++) {
            var attr = attributes[j];
            if (_.indexOf(attributesToRemove, attr.FieldName) === -1) {
                remainingAttributes.push(attr);
            }
        }
        return { removedAttributes: removedAttributes, remainingAttributes: remainingAttributes };
    }

    function getAttribute(attributes, fieldName) {
        return _.findWhere(attributes, { FieldName: fieldName });
    }

    function sortAttributes(attributes) {
        return _.sortBy(attributes, 'DisplayName');
    }

    function getFieldName(attributeItem) {
        return attributeItem.attr('fn');
    }

    function showAttributeHover(target, $event) {
        $scope.attributeHoverTimeout = setTimeout(function() {
            if (target.offset().top <= 0) {
                return;
            }

            var hover = $scope.attributeHover;
            hover[0].style.opacity = 0;
            hover.removeClass('hide');

            var container = $scope.leadEnrichment;
            var top = target.offset().top + target.height() + container.scrollTop() - container.position().top + 10;
            if (top < 0) {
                top = 0;
            }
            hover.css('top', top);

            var left = $event.pageX + container.scrollLeft() - parseInt(hover.width() / 3);
            var minLeft = target.offset().left + 20;
            if (minLeft < 0) {
                minLeft = 0;
            }
            if (left < minLeft) {
                left = minLeft;
            }
            hover.css('left', left);

            hover[0].style.opacity = 1;
        }, 400);
    }

    function hideAttributeHover() {
        if ($scope.attributeHoverTimeout) {
            clearTimeout($scope.attributeHoverTimeout);
        }
        $scope.attributeHover.addClass('hide');
    }
});