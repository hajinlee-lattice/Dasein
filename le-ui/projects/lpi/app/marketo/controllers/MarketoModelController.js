angular.module('lp.marketo.models', [])
.component('marketoActiveModels', {
    templateUrl: 'app/marketo/views/MarketoActiveModelsView.html',
    controller: function( 
        $q, $state, $stateParams, $scope, $timeout, $filter, BrowserStorageUtility, MarketoStore) 
    { 
        var vm = this,
            resolve = $scope.$parent.$resolve,
            activeModels = resolve.ActiveModels,
            scoringRequestSummaries = resolve.ScoringRequestSummaries;
        
        angular.extend(vm, {
            showWebhookLink: {},
            credentialId: $stateParams.credentialId
        });

        vm.init = function() {
            console.log(activeModels);
            vm.activeModels = activeModels;
            MarketoStore.setActiveModels(vm.activeModels);
            vm.scoringRequestSummaryIds = new Set(scoringRequestSummaries.map(function(scoringRequest) {return scoringRequest.modelUuid;}))
        }

        vm.getConfigId = function(modelId) {
            var model = scoringRequestSummaries.find(function(scoringRequest) {
                return scoringRequest.modelUuid === modelId;
            });
            if (model) {
                return model.configId;
            } else {
                console.warn('Could not find scoring request with modelId: ' + modelId);
            }

        }

        vm.init();
    }
})
.component('marketoSetupModel', {
    templateUrl: 'app/marketo/views/MarketoModelSetupView.html',
    controller: function( 
        $q, $state, $stateParams, $scope, $timeout, $filter, BrowserStorageUtility, MarketoStore, MarketoService) 
    { 
        var vm = this,
            resolve = $scope.$parent.$resolve;

        angular.extend(vm, {
            modelId: $stateParams.modelUuid,
            credentialId: $stateParams.credentialId,
            marketoFields: resolve.MarketoFields.data,
            primaryFields: resolve.PrimaryAttributeFields,
            scoringFields: resolve.ScoringFields,
            currentScoringRequest: resolve.ExistingScoringRequest,
            marketoDisplayFieldNames: [],
            fieldsMapping: {},
            requiredMatchFields: ['CompanyName', 'DUNS', 'Email'],
            defaultMapping: {
                FirstName: "Lead.First Name",
                LastName: "Lead.Last Name",
                Industry: "Company.Industry",
                Title: "Lead.Job Title"
            }
        });

        vm.init = function() {
            vm.marketoFieldsSet = new Set(vm.marketoFields.map(function(marketoField) {
                return marketoField.apiName;
            }))
            vm.checkEnableSave();
            vm.updateMarketoScoringMatchFields();
            vm.mapDefaultMatchFields();
        }

        vm.isRequiredField = function(field) {
            return vm.requiredMatchFields.indexOf(field.fieldName) >= 0;
        }

        vm.mapDefaultMatchFields = function() {
            var defaultFields = $filter('filter')(vm.scoringFields.fields, vm.isDefaultField, true);

            defaultFields.forEach(function(defaultField) {
                if (vm.marketoFieldsSet.has(vm.defaultMapping[defaultField.fieldName]) && vm.fieldsMapping[defaultField.fieldName] == undefined) {
                    vm.fieldsMapping[defaultField.fieldName] = vm.defaultMapping[defaultField.fieldName];
                }
            });
        }

        vm.isDefaultField = function(field) {
            return vm.defaultMapping[field.fieldName] != undefined;
        }

        vm.orderFieldNames = function(field) {
            return !vm.isDefaultField(field);
        }

        vm.updateMarketoScoringMatchFields = function() {
            if (vm.currentScoringRequest) {
                vm.currentScoringRequest.marketoScoringMatchFields.forEach(function(mapping) {
                    vm.fieldsMapping[mapping.modelFieldName] = mapping.marketoFieldName;
                });
            }
        }

        vm.checkEnableSave = function() {
            vm.enableSave = false;
            vm.requiredMatchFields.forEach(function(field) {
                if (vm.fieldsMapping[field] != undefined && vm.fieldsMapping[field] != '') {
                    vm.enableSave = true;
                    return;
                } 
            });
        }

        vm.createOrUpdateScoringRequest = function() {
            var marketoScoringMatchFields = [];
            Object.keys(vm.fieldsMapping).forEach(function(key) {
                if (vm.fieldsMapping[key]) {
                    marketoScoringMatchFields.push({
                        modelFieldName: key,
                        marketoFieldName: vm.fieldsMapping[key]                       
                    });
                }
            })

            if (!vm.currentScoringRequest) { // CREATE
                var scoringRequest = {
                    modelUuid: vm.modelId,
                    marketoScoringMatchFields: marketoScoringMatchFields
                };

                MarketoService.CreateScoringRequest(vm.credentialId, scoringRequest).then(function(result){
                    console.log(result);
                    MarketoStore.setScoringRequest(result);
                    $state.go('home.marketosettings.webhook', {"credentialId": $stateParams.credentialId, "configId": result.configId});
                });
            } else { // UPDATE
                var scoringRequest = {
                    configId: vm.currentScoringRequest.configId,
                    modelUuid: vm.modelId,
                    marketoScoringMatchFields: marketoScoringMatchFields
                };
                MarketoService.UpdateScoringRequest(vm.credentialId, scoringRequest).then(function(result) {
                    console.log(result);
                    MarketoStore.updateScoringRequestMatchFields(scoringRequest);
                    $state.go('home.marketosettings.webhook', {"credentialId": $stateParams.credentialId, "configId": vm.currentScoringRequest.configId});
                });
            }
        }

        vm.init();
    }
})
.component('marketoWebhookSummary', {
    templateUrl: 'app/marketo/views/MarketoWebhookSummaryView.html',
    controller: function( 
        $q, $state, $stateParams, $scope, $timeout, BrowserStorageUtility) 
    {
        var vm = this,
            resolve = $scope.$parent.$resolve,
            model = resolve.Model,
            scoringRequest = resolve.ScoringRequest,
            credential = resolve.MarketoCredentials,
            backState = resolve.BackState;

        angular.extend(vm, {
            credentialId: $stateParams.credentialId,
            webhookName: model.ModelDetails.DisplayName,
            url: scoringRequest.webhookResource + $stateParams.configId,
            marketoScoringMatchFields: scoringRequest.marketoScoringMatchFields,
            secretKey: credential ? credential.lattice_secret_key : 'null',
            rule: "{{campaign.name}}",
            responseMappingText: "<Select matching Marketo field>",
            backState: backState || "home.marketosettings.scoring",
            modelUuid: scoringRequest.modelUuid

        });

        vm.init = function() {
            vm.setJsonTemplate();
        }

        vm.setJsonTemplate = function() {
            vm.template = {};
            vm.marketoScoringMatchFields.forEach(function(mapping){
                vm.template[mapping.modelFieldName] = '{{' + mapping.marketoFieldName + '}}';
            })

        }

        vm.init();
    }
});
