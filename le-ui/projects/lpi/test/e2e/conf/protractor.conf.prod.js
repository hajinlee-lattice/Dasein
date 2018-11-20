'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantId = "ProductionTestPLSTenant2.ProductionTestPLSTenant2.Production";
config.params.alternativeTenantId = "ProductionTestPLSTenant1.ProductionTestPLSTenant1.Production";
config.params.isProd = true;

var specs = [];
var excludedSpecs = ['setup_spec.js'];
config.specs.forEach(function(spec){
    var shouldExclude = false;
    for (var i = 0; i < excludedSpecs.length; i++) {
        if (spec.indexOf(excludedSpecs[i]) > -1) {
            shouldExclude = true;
            break;
        }
    }
    if (!shouldExclude) {
        specs.push(spec);
    }
});
config.specs = specs;

exports.config = config;