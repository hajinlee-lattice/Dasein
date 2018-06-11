exports.config = {
    allScriptsTimeout: 11000,
    
    specs: [
        'e2e/*.js'
    ],
    
    // TODO: add back firefox
    // once this issue is fixed: https://github.com/teerapap/grunt-protractor-runner/issues/88
    // TODO: look into IE support: 
    // https://code.google.com/p/selenium/wiki/InternetExplorerDriver
    multiCapabilities: [{
        'browserName': 'chrome'
    }],
    
    baseUrl: 'http://localhost:8000/',
    
    framework: 'jasmine',
    
    jasmineNodeOpts: {
        defaultTimeoutInterval: 30000
    }
};