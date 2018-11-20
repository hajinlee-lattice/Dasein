'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantId = "QATestPLSTenant1.QATestPLSTenant1.Production";
config.params.alternativeTenantId = "QATestPLSTenant2.QATestPLSTenant2.Production";
config.params.appUrl = 'http://localhost:3000';

exports.config = config;