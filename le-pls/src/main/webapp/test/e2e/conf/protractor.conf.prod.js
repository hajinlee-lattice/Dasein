'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantId = "ProductionTestPLSTenant2.ProductionTestPLSTenant2.Production";
config.params.alternativeTenantId = "ProductionTestPLSTenant1.ProductionTestPLSTenant1.Production";

exports.config = config;