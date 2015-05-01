exports.config = {
    // Spec patterns are relative to the current working directly when
    // protractor is called.
    specs: [
        'src/test/webapp/e2e/mainflow_spec.js'
    ],

    params: {
        adtesterusername: "testuser1",
        adtesterpassword: "Lattice1"
    },

    allScriptsTimeout: 60000,

    jasmineNodeOpts: {
        showColors: true,
        defaultTimeoutInterval: 60000
    },

    onPrepare: function() {
        var jasmineReporters = require('jasmine-reporters');
        var junitReporter = new jasmineReporters.JUnitXmlReporter(
            'target/protractor-test-results', true, true, 'protractor-test-results', true
        );
        jasmine.getEnv().addReporter(junitReporter);
    }
};
