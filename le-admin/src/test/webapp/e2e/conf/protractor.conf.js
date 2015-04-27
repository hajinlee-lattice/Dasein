exports.config = {
    // Spec patterns are relative to the current working directly when
    // protractor is called.
    specs: [
        '../**/mainflow_spec.js'
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
    	require('jasmine-reporters');
	    jasmine.getEnv().addReporter(
	        new jasmine.JUnitXmlReporter('target/protractor-test-results', true, true, 'protractor-test-results', true));
    }
};
