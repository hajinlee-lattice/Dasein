'use strict';

var Config = function() {

    this.generateConfig = function(specBasePath) {

        var chromeDownloadPath;
        if (process.platform.indexOf('win') > -1) {
            chromeDownloadPath = process.env['HOMEDRIVE'] + process.env['HOMEPATH'] + '\\Downloads\\';
        } else {
            chromeDownloadPath = process.env['HOME'] + '/Downloads/';
        }

        return {
            specs: [
                specBasePath + '/mainflow_spec.js',
                specBasePath + '/modellist_spec.js',
                specBasePath + '/modeldetail_spec.js',
                specBasePath + '/predictors_spec.js',
                specBasePath + '/thresholdexplorer_spec.js',
                specBasePath + '/leadsample_spec.js',
                specBasePath + '/admininfo_spec.js',
                specBasePath + '/usermgmt_spec.js',
                specBasePath + '/activatemodel_spec.js',
                specBasePath + '/systemsetup_spec.js',
                specBasePath + '/passwordchange_spec.js',
                specBasePath + '/multitenant_spec.js',
                specBasePath + '/internaladmin_spec.js',
                specBasePath + '/managefields_spec.js',
                specBasePath + '/tenantdeployment_spec.js',
                specBasePath + '/leadenrichment_spec.js',
                specBasePath + '/systemsetup_spec.js'
            ],

            params: {
                superAdminUsername:     'pls-super-admin-tester@test.lattice-engines.com',
                internalAdminUsername:  'pls-internal-admin-tester@test.lattice-engines.com',
                internalUserUsername:   'pls-internal-user-tester@test.lattice-engines.com',
                externalAdminUsername:  'pls-external-admin-tester@test.lattice-engines.ext',
                externalUserUsername:   'pls-external-user-tester@test.lattice-engines.ext',
                testingUserPassword:    'admin',
                downloadRoot:           chromeDownloadPath,
                isProd:                 false
            },

            allScriptsTimeout: 200000,

            jasmineNodeOpts: {
                showColors: true,
                defaultTimeoutInterval: 300000
            },

            framework: "jasmine2",

            onPrepare: function() {
                var jasmineReporters = require('jasmine-reporters');
                var junitReporter = new jasmineReporters.JUnitXmlReporter({
                    consolidateAll: true,
                    filePrefix: 'protractor',
                    savePath: 'target/protractor-test-results'
                });
                jasmine.getEnv().addReporter(junitReporter);
            }
        };

    };

};

module.exports = new Config();
