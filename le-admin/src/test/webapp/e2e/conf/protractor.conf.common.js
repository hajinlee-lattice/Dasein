'use strict';

var Config = function() {

    this.generateConfig = function(specBasePath) {

        return {
            specs: [
                specBasePath + '/mainflow_spec.js'
            ],

            params: {
                adtesterusername: "testuser1",
                adtesterpassword: "Lattice1"
            },

            allScriptsTimeout: 110000,

            jasmineNodeOpts: {
                showColors: true,
                defaultTimeoutInterval: 120000
            },

            onPrepare: function() {
                var jasmineReporters = require('jasmine-reporters');
                var junitReporter = new jasmineReporters.JUnitXmlReporter(
                    'target/protractor-test-results', true, true, 'protractor-test-results', true
                );
                jasmine.getEnv().addReporter(junitReporter);
            }
        };

    };

};

module.exports = new Config();