angular.module('pd.markets.createmarket', [
        'pd.builder.attributes'
    ])
    .controller('CreateMarketCtrl', function(AttributesModel) {
        this.AttributesModel = AttributesModel;
        AttributesModel.TargetMarketName = 'Target Market #' + parseInt(Math.random() * 99999);
    });