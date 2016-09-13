angular.module('lp.marketo.enrichment', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('MarketoEnrichmentController', function($scope, $timeout, $state, $stateParams, BrowserStorageUtility, EnrichmentData){
    var vm = this;

    angular.extend(vm, {
        params: $stateParams,
        save_ready: false,
        saved: false,
        step: 1,
        marketo_fields: null,
        required_fields: [],
        selected_fields: {},
        match_fields: {},
        pagesize: 5,
    });

    vm.webhook_name = 'name';
    vm.webhook_url = 'url';
    vm.webhook_request_type = 'type';
    vm.webhook_request_token = 'token';
    vm.webhook_response_type = 'type';
    vm.custom_header_type = 'type';
    vm.custom_header_value = 'value';

    vm.match_fields = {
        email_or_website: {
            label: 'Email or Website',
            required: true,
            options: [
                'email@mail.me',
                'http://www.website.me'
            ]
        },
        company: {
            label: 'Company',
            options: [
                'Company, LLC',
                'Alone, INC.'
            ]
        },
        state: {
            label: 'State',
            options: [
                'Graceland',
                'Eldorado'
            ]
        },
        country: {
            label: 'Country',
            options: [
                'Elviso',
                'USA'
            ]
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
    
    vm.save = function() {
        _.each(vm.match_fields, function(value, key){
            vm.selected_fields[key] = vm.marketo_field[key] || null;
        })
        vm.saved = true;
        vm.open({step: 2});
    }

    vm.changeField = function(type) {
        var type = type || '',
            value = vm.marketo_field[type],
            required = vm.match_fields[type].required;

        if(required) {
            if(value) {
                var index = vm.required_fields.indexOf(type);
                vm.required_fields.splice(index, 1);
            } else {
                if(vm.required_fields.indexOf(type) == -1) {
                    vm.required_fields.push(type);
                }
            }
        }
        if(vm.required_fields.length) {
            vm.save_ready = false;
        } else {
            vm.save_ready = true;
        }
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

    vm.init = function() {
        _.each(vm.match_fields, function(field, key){
            if(field.required && vm.required_fields.indexOf(key) == -1) {
                vm.required_fields.push(key);
            }
        });
        vm.enrichments = EnrichmentData.data;
    }

    vm.init();
});
