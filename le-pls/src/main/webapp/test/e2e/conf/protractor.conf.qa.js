'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantId = "QATestPLSContract.Tenant2.Production";
config.params.alternativeTenantId = "QATestPLSContract.Tenant1.Production";

exports.config = config;