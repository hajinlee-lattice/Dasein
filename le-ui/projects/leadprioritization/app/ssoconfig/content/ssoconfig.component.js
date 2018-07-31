angular.module('lp.ssoconfig.configure', [])
.component('ssoConfig', {
    templateUrl: 'app/ssoconfig/content/ssoconfig.component.html',
    controller: function( 
        $q, $state, $stateParams, $scope, $location, $timeout, SSOConfigStore, ImportStore, Banner) 
    { 
        var vm = this,
            resolve = $scope.$parent.$resolve,
            SSOConfiguration = resolve.SSOConfiguration,
            ServiceProviderURLs = resolve.ServiceProviderURLs;

        console.log(SSOConfiguration);
        angular.extend(vm, {
            isConfigured: SSOConfiguration && typeof SSOConfiguration.errorCode == 'undefined',
            configId: SSOConfiguration ? SSOConfiguration.config_id : '',
            samlVersion: 'n/a',
            status: 'n/a',
            metadataUploadDate: 'n/a',
            metadataXMLLabel: 'n/a',
            metadata: {},
            startURL: ServiceProviderURLs['assertionConsumerServiceURL'] ? ServiceProviderURLs['assertionConsumerServiceURL'] :'http://app.lattice-engines.com/sso',
            acsURL: ServiceProviderURLs['assertionConsumerServiceURL'] ? ServiceProviderURLs['assertionConsumerServiceURL'] :'http://app.lattice-engines.com/sso',
            entityID: ServiceProviderURLs['serviceProviderEntityId'] ? ServiceProviderURLs['serviceProviderEntityId'] :'http://app.lattice-engines.com/sso',
            logoutURL: 'http://app.lattice-engines.com/logoutApp',
            iconURL: $location.protocol() + "://" + location.host + '/images/logo-lattice-color.png',
            uploading: false,
            canSubmit: false,
            isValidMetadata: false,
            isTouched: false,
            params: {
                compressed: true,
                importError: false,
                importErrorMsg: ''
            }
        });

        vm.init = function() {
            vm.setConfigTable();
        }

        vm.setConfigTable = function() {
            if (vm.configId) {
                vm.samlVersion = 'V 2.0';
                vm.status = 'Success';
                vm.metadataUploadDate = SSOConfiguration.created;
                vm.metadataXMLLabel = 'Download File'
            }
        }

        vm.copyLink = function(link) {
            var element = $('#' + link);
            var $temp = $("<input>");
            $("body").append($temp);
            $temp.val($(element).text()).select();
            document.execCommand("copy");
            $temp.remove();
        }

        vm.downloadMetadata = function() {
            var data, filename, link;
            filename = 'metadata.xml';

            var xml = SSOConfiguration.metadata;
            if (!xml.match(/^data:text\/xml/i)) {
                xml = 'data:text/xml;charset=utf-8,' + xml;
            }
            data = encodeURI(xml);

            link = document.createElement('a');
            link.setAttribute('href', data);
            link.setAttribute('download', filename);
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        }

        vm.fileLoad = function(headers) {
            vm.uploading = true;
            vm.isTouched = true;
        }

        vm.fileSelect = function(fileName) {
        }

        vm.fileDone = function(result) {
            vm.uploading = false;
        }
        
        vm.fileCancel = function() {
            vm.uploading = false;
            var xhr = ImportStore.Get('cancelXHR', true);
            
            if (xhr) {
                xhr.abort();
            }
        }

        vm.fileValidation = function(result) {
            Banner.reset();
            return vm.validateMetadata({metadata: result});
            
        }

        vm.validateMetadata = function(config) {
            var deferred = $q.defer();
            SSOConfigStore.validateSAMLConfig(config).then(function(validation) {
                vm.isValidMetadata = vm.canSubmit = validation.valid;
                vm.metadata = validation.valid ? config : {}; 
                if (!validation.valid) {
                    Banner.error({title: 'Validation Error', message: validation.exceptionMessage});
                }
                deferred.resolve(validation.valid);
            });
            return deferred.promise;
        }

        vm.submitMetadata = function() {
            if (vm.configId && vm.isValidMetadata) {
                SSOConfigStore.deleteSAMLConfig(vm.configId).then(function(result) {
                    SSOConfigStore.createSAMLConfig(vm.metadata).then(function(result) {
                        Banner.success({title: 'Success!', message: 'Your SAML is configured.'});
                    });
                });
            } else {
                SSOConfigStore.createSAMLConfig(vm.metadata).then(function(result) {
                    Banner.success({title: 'Success!', message: 'Your SAML is configured.'});
                });
            }
        }

        vm.init();
    }
});