'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('src/main/webapp/test/e2e');

config.params.tenantId = "QATestPLSTenant2.QATestPLSTenant2.Production";
config.params.alternativeTenantId = "QATestPLSTenant1.QATestPLSTenant1.Production";

exports.config = config;