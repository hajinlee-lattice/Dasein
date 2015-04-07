exports.config = {
    // Spec patterns are relative to the current working directly when
    // protractor is called.
    specs: ['../**/*_spec.js'],
    //specs: ['../**/mainflow_spec.js'],
    //specs: ['../**/usermgmt_spec.js'],
    //specs: ['../**/modellist_spec.js'],
    //specs: ['../**/leadsample_spec.js'],

	params: {
		tenantIndex:            0,
        alternativeTenantIndex: 1,
        adminDisplayName:       'Super User',
        adminUsername:          'bnguyen@lattice-engines.com',
        adminPassword:          'tahoe',
        nonAdminDisplayName:    'General User',
        nonAdminUsername:       'lming@lattice-engines.com',
        nonAdminPassword:       'admin'
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
