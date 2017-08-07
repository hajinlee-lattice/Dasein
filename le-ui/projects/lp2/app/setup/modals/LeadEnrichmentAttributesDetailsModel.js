angular.module('mainApp.setup.controllers.LeadEnrichmentAttributesDetailsModel', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.services.SessionService',
    'mainApp.setup.services.LeadEnrichmentService'
])

.service('LeadEnrichmentAttributesDetailsModel', function ($compile, $rootScope, $http) {
    this.show = function($parentScope, title, attributes) {
        $http.get('./app/setup/views/LeadEnrichmentAttributesDetailsView.html').success(function (html) {
            var scope = $parentScope.$new();
            scope.title = title;
            scope.originalAttributes = attributes;
            scope.hasAttributes = (attributes != null && attributes.length > 0);
            var contentContainer = $('#leadEnrichmentAttributesDetails');
            $compile(contentContainer.html(html))(scope);
        });
    };
})

.controller('LeadEnrichmentAttributesDetailsController', function ($scope, $rootScope, $http, _, ResourceUtility, BrowserStorageUtility, NavUtility, SessionService, ConfigService, LeadEnrichmentService) {
    $scope.ResourceUtility = ResourceUtility;
    if ($scope.hasAttributes) {
        var templateType = $('#leadEnrichmentAttributesDetails').attr('templatetype');
        if (templateType != null && templateType !== '') {
            generateAttributes(templateType);
        } else {
            $scope.loading = true;
            LeadEnrichmentService.GetTemplateType().then(function(result) {
                if (result.Success === true) {
                    $('#leadEnrichmentAttributesDetails').attr('templatetype', result.ResultObj);
                    generateAttributes(result.ResultObj);
                } else {
                    $scope.showLoadingError = true;
                    $scope.loadingError = result.ResultErrors;
                }
                $scope.loading = false;
            });
        }
    }

    function generateAttributes(templateType) {
        var attributes = [$scope.originalAttributes.length];
        for (var i = 0; i < $scope.originalAttributes.length; i++) {
            var attribute = {};
            var originalAttribute = $scope.originalAttributes[i];
            attribute.name = originalAttribute.DisplayName;
            attribute.dataSource = originalAttribute.DataSource;
            attribute.fieldName = 'Lattice_' + originalAttribute.FieldNameInTarget;
            attribute.fieldType = convertFieldType(templateType, originalAttribute.FieldType);
            attributes[i] = attribute;
        }
        $scope.attributes = attributes;
    }

    function convertFieldType(templateType, fieldType) {
        var dataType = fieldType;
        if (dataType != null) {
            var dataTypeUpperCase = dataType.toUpperCase();
            if (templateType != null && templateType.toUpperCase() === 'MKTO') {
                if (dataTypeUpperCase.indexOf('NVARCHAR') > -1) {
                    dataType = dataType.replace(/NVARCHAR/i, 'String');
                } else if (dataTypeUpperCase.indexOf('VARCHAR') > -1) {
                    dataType = dataType.replace(/VARCHAR/i, 'String');
                } else if (dataTypeUpperCase.indexOf('FLOAT') > -1) {
                    dataType = 'Float';
                } else if (dataTypeUpperCase === 'INT') {
                    dataType = 'Integer';
                }
            } else {
                if (dataTypeUpperCase.indexOf('NVARCHAR') > -1) {
                    dataType = dataType.replace(/NVARCHAR/i, 'Text');
                } else if (dataTypeUpperCase.indexOf('VARCHAR') > -1) {
                    dataType = dataType.replace(/VARCHAR/i, 'Text');
                } else if (dataTypeUpperCase.indexOf('FLOAT') > -1 || dataTypeUpperCase === 'INT') {
                    dataType = 'Number';
                }
            }
        }
        return dataType;
    }

    $scope.backButtonClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.attributes = null;
        $scope.originalAttributes = null;
        $scope.$parent.setAttributesDetails(false);
    };
});