'use strict';

var Config = function() {

    this.generateConfig = function(specBasePath) {

        return {
            specs: [
                specBasePath + '/mainflow_spec.js',
                specBasePath + '/modellist_spec.js',
                specBasePath + '/modeldetail_spec.js',
                specBasePath + '/predictors_spec.js',
                specBasePath + '/thresholdexplorer_spec.js',
                specBasePath + '/leadsample_spec.js',
                specBasePath + '/usermgmt_spec.js'
            ],

            params: {
                superAdminUsername:     'pls-super-admin-tester@test.lattice-engines.com',
                internalAdminUsername:  'pls-internal-admin-tester@test.lattice-engines.com',
                internalUserUsername:   'pls-internal-user-tester@test.lattice-engines.com',
                externalAdminUsername:  'pls-external-admin-tester@test.lattice-engines.ext',
                externalUserUsername:   'pls-external-user-tester@test.lattice-engines.ext',
                testingUserPassword:    'admin'
            },

            allScriptsTimeout: 110000,

            jasmineNodeOpts: {
                showColors: true,
                defaultTimeoutInterval: 120000
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