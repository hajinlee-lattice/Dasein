'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantName = "QATest Tenant 2";
config.params.alternativeTenantName = "QATest Tenant 1";

exports.config = config;