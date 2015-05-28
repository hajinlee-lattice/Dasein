'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('src/main/webapp/test/e2e');

config.params.tenantId = "QATestPLSContract.QATestTenant2.Production";
config.params.alternativeTenantId = "QATestPLSContract.QATestTenant1.Production";

exports.config = config;