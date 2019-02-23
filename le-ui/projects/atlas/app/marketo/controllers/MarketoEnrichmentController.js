angular.module('lp.marketo.enrichment', [
    'common.utilities.browserstorage'
])
.controller('MarketoEnrichmentController', function($scope, $interval, $timeout, $state, $stateParams, $filter, $q,
    BrowserStorageUtility, EnrichmentData, MarketoCredential, MarketoMatchFields, MarketoService) {
    var vm = this;

    angular.extend(vm, {
        params: $stateParams,
        save_ready: false,
        saved: false,
        step: 1,
        marketo_fields: null,
        marketo_field: [],
        required_fields: [],
        selected_fields: {},
        match_fields: {},
        enrichments: EnrichmentData,
        marketoEnrichments: MarketoCredential.enrichment, // Match Fields, labels
        marketoMatchFields: MarketoMatchFields, // Marketo Fields, dropdowns
        pagesize: 5
    });

    vm.webhook_name = 'Lattice Lead Enrichment';
    vm.webhook_url = vm.marketoEnrichments.webhook_url + vm.marketoEnrichments.tenant_credential_guid;
    vm.webhook_request_type = 'POST';
    vm.webhook_request_token = 'JSON';
    vm.webhook_response_type = 'JSON';
    vm.custom_header_type = 'Content-Type';
    vm.custom_header_value = 'application/json';

    vm.match_fields = {
        Domain: {
            label: 'Email or Website',
            required: true,
            data: vm.marketoEnrichments.marketo_match_fields[0],
            options: vm.marketoMatchFields
        },
        Company: {
            label: 'Company',
            data: vm.marketoEnrichments.marketo_match_fields[1],
            options: vm.marketoMatchFields
        },
        State: {
            label: 'State',
            data: vm.marketoEnrichments.marketo_match_fields[2],
            options: vm.marketoMatchFields
        },
        Country: {
            label: 'Country',
            data: vm.marketoEnrichments.marketo_match_fields[3],
            options: vm.marketoMatchFields
        },DUNS: {
            label: 'DUNS',
            data: vm.marketoEnrichments.marketo_match_fields[4],
            options: vm.marketoMatchFields
        }
    }

    vm.open = function(opts) {
        var opts = opts || {};
        if(opts.step) {
            vm.step = opts.step;
        } else if(opts.event) {
            var section = angular.element(opts.event.target).closest('section').first(),
                classes = section.attr('class').split(' '),
                step;
            
            _.each(classes, function(classname){ 
                var classname = String(classname)
                if(classname.includes('step-'))  {
                    step = classname.replace('step-',''); 
                }
            });
            if(vm.saved) {
                vm.step = step;
            } else {
                step = 1;
            }
        }
    }

    vm.selected_options = {};
    vm.initial_options = {};
    vm.disabled_options = [];
    _.each(vm.match_fields, function(value, key){
        if(value.data.marketoFieldName) {
            vm.selected_options[key] = value.data.marketoFieldName;
            vm.initial_options[key] = value.data.marketoFieldName;
            vm.disabled_options.push(value.data.marketoFieldName);
        }
    })
    vm.selectOption = function(type, option) {
        vm.selected_options[type] = option;
        vm.disabled_options = [];
        _.each(vm.selected_options, function(value, key){
            if(!vm.disabled_options.includes(value)) {
                vm.disabled_options.push(value);
            }
        });
    }

    vm.changeField = function(type) {
        vm.saved = false;
        if(vm.marketo_field[type] === vm.initial_options[type]){
            vm.saved = true;
        }
        for( var i in vm.marketo_field) {
            if(vm.marketo_field[i] && vm.marketo_field[i] !== vm.initial_options[i]){
                vm.saved = false;
                break;
            } else if(vm.marketo_field[i] && vm.marketo_field[i] === vm.initial_options[i]){
                vm.saved = true;
            }
        }
        var type = type || '',
            value = vm.marketo_field[type],
            required = vm.match_fields[type].required;

        if(required) {
            if(vm.marketo_field[type]) {
                var index = vm.required_fields.indexOf(type);
                vm.required_fields.splice(index, 1);
            } else {
                if(vm.required_fields.indexOf(type) == -1) {
                    vm.required_fields.push(type);
                }
            }
        }

        if(vm.required_fields.length && !vm.save_ready) {
            vm.save_ready = false;
        } else {
            vm.save_ready = true;
        }
    }
    
    var UpdateEnrichmentFields = function(credentialId, marketoMatchFields) {
        var deferred = $q.defer();
        
        MarketoService.UpdateEnrichmentFields(credentialId, marketoMatchFields).then(function(result) {
            deferred.resolve(result);
        });

        return deferred.promise;
    }

    vm.save = function() {
        var saved_marketoMatchFields = [];
        console.log(vm.marketo_field);
        _.each(vm.match_fields, function(value, key){
            vm.selected_fields[key] = addBrackets(vm.marketo_field[key]) || null;
            saved_marketoMatchFields.push({
                marketoFieldName: vm.marketo_field[key],
                marketo_match_field_name: value.data.marketo_match_field_name,
            });
        })
        UpdateEnrichmentFields(vm.params.id, saved_marketoMatchFields);
        vm.saved = true;
        vm.open({step: 2});
    }

    vm.fieldType = function(fieldType){
        var fieldType = fieldType.replace(/[0-9]+/g, '*');
        var fieldTypes = {
            'default':'Text/String',
            'NVARCHAR(*)':'Text/String',
            'FLOAT':'Number/Float',
            'INT':'Number/Int',
            'BOOLEAN':'Boolean'
        }
        return fieldTypes[fieldType] || fieldTypes.default;
    }

    var setOptionsSelectedState = $interval(function(){
        var has_selected = false,
            are_selected = false;
        _.each(vm.marketoEnrichments.marketo_match_fields, function(value, key){
            if(value.marketoFieldName) {
                has_selected = true;
            }
        })
        var selected = document.querySelectorAll('option[selected="selected"]');
        _.each(selected, function(value, key){
            vm.marketo_field[value.parentNode.name] = value.value;
            //value.selected = true;
            are_selected = true;
            if(value.parentElement.attributes.required) {
                vm.save_ready = true;
            }
        });
        if(has_selected && are_selected) {
            $interval.cancel(setOptionsSelectedState);
        }
    }, 1000);

    var addBrackets = function(string){
        if(string) {
            return '{{' + string + '}}';
        }
        return null;
    }

    vm.init = function() {
        _.each(vm.match_fields, function(value, key){
            if(value.required && vm.required_fields.indexOf(key) == -1) {
                vm.required_fields.push(key);
            }
            vm.selected_fields[key] = addBrackets(value.data.marketoFieldName) || null;
            if(value.data.marketoFieldName) {
                vm.saved = true;
            }
        });
    }

    vm.init();
});
