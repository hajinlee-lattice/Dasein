exports.config = {
    // Spec patterns are relative to the current working directly when
    // protractor is called.
    //specs: ['../**/*_spec.js'],
    specs: [
        '../**/mainflow_spec.js',
        '../**/modellist_spec.js',
        '../**/modeldetail_spec.js',
        '../**/predictors_spec.js',
        '../**/thresholdexplorer_spec.js',
        '../**/leadsample_spec.js',
        '../**/usermgmt_spec.js'
    ],

	params: {
		tenantIndex:            1,
        alternativeTenantIndex: 0,
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

    onPrepare: function() {
    	require('jasmine-reporters');
	    jasmine.getEnv().addReporter(
	        new jasmine.JUnitXmlReporter('target/protractor-test-results', true, true, 'protractor-test-results', true));
    }
};
