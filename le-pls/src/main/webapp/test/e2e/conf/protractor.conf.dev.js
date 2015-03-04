exports.config = {
    // Spec patterns are relative to the current working directly when
    // protractor is called.
    specs: ['../**/*_spec.js'],

	params: {
		tenantIndex: 1
	},

    onPrepare: function() {
    	require('jasmine-reporters');
	    jasmine.getEnv().addReporter(
	        new jasmine.JUnitXmlReporter('target/protractor-test-results', true, true, 'protractor-test-results', true));	    
    }
};
