'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('src/main/webapp/test/e2e');

config.params.tenantId = "IntegrationTestPLSContract.IntegrationTestTenant2.Production";
config.params.alternativeTenantId = "IntegrationTestPLSContract.IntegrationTestTenant1.Production";

exports.config = config;