angular.module('lp.ssoconfig.configure', [])
.component('ssoConfig', {
    templateUrl: 'app/ssoconfig/content/ssoconfig.component.html',
    controller: function( 
        $q, $state, $stateParams, $scope, $location, $timeout, BrowserStorageUtility, SSOConfigStore, ImportStore, Banner) 
    { 
        var vm = this,
            resolve = $scope.$parent.$resolve,
            SSOConfiguration = resolve.SSOConfiguration,
            ServiceProviderURLs = resolve.ServiceProviderURLs;

        angular.extend(vm, {
            configuration: SSOConfiguration,
            canEdit: BrowserStorageUtility.getClientSession().AvailableRights.PLS_SSO_Config.MayEdit,
            isConfigured: SSOConfiguration && typeof SSOConfiguration.config_id != 'undefined',
            configId: SSOConfiguration ? SSOConfiguration.config_id : '',
            samlVersion: 'n/a',
            status: 'n/a',
            metadataUploadDate: 'n/a',
            metadataXMLLabel: 'n/a',
            metadata: {},
            startURL: ServiceProviderURLs['assertionConsumerServiceURL'],
            acsURL: ServiceProviderURLs['assertionConsumerServiceURL'],
            entityID: ServiceProviderURLs['serviceProviderEntityId'],
            logoutURL: 'http://app.lattice-engines.com/logoutApp',
            iconURL: '',
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
            var parser = document.createElement('a');
            parser.href = vm.startURL;
            vm.iconURL = parser.protocol + '//' + parser.hostname + '/images/lattice-logo.png';
            vm.setConfigTable(vm.configuration);
        }

        vm.setConfigTable = function(configuration) {
            if (vm.isConfigured) {
                vm.samlVersion = 'V 2.0';
                vm.status = 'Success';
                vm.metadataUploadDate = configuration.created;
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

            var xml = vm.configuration.metadata;
            if (!xml.match(/^data:text\/xml/i)) {
                xml = 'data:text/xml;charset=utf-8,' + xml;
            }
            data = encodeURI(xml);
            data = data.replace(/#/g, '%23');

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
                    Banner.error({message: "Failed! Your metadata has unexpected errors"});
                }
                deferred.resolve(validation.valid);
            });
            return deferred.promise;
        }

        vm.submitMetadata = function() {
            if (vm.configId && vm.isValidMetadata) {
                SSOConfigStore.deleteSAMLConfig(vm.configId).then(function(result) {
                    SSOConfigStore.createSAMLConfig(vm.metadata).then(function(result) {
                        if (typeof result.errorCode == 'undefined') {
                            Banner.success({title: 'Success!', message: 'Your SAML is configured.'});
                        }
                        vm.updateConfigTable();
                    });
                });
            } else {
                SSOConfigStore.createSAMLConfig(vm.metadata).then(function(result) {
                    if (typeof result.errorCode == 'undefined') {
                        Banner.success({title: 'Success!', message: 'Your SAML is configured.'});    
                    }
                    vm.updateConfigTable();
                });
            }
        }

        vm.updateConfigTable = function() {
            SSOConfigStore.getSAMLConfig().then(function(result) {
                vm.isConfigured = result && typeof result.config_id != 'undefined';
                vm.configId = vm.isConfigured ? result.config_id : '';
                vm.configuration = result ? result : {};
                if (vm.configId) {
                    vm.setConfigTable(result);
                } else {
                    vm.samlVersion = 'V 2.0';
                    vm.status = 'Failed';
                    vm.metadataUploadDate = result && result.created ? result.created : 'n/a';
                    vm.metadataXMLLabel = result && result.metadata ? 'Download File' : 'n/a';
                    vm.params.scope.cancel();
                    vm.canSubmit = false;
                }
            });
        }

        vm.init();
    }
});