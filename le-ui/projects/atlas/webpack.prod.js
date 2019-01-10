const webpack = require('webpack');
const merge = require('webpack-merge');
const UglifyJsPlugin = require('uglifyjs-webpack-plugin');

const common = require('./webpack.common.js');

module.exports = merge(common, {
    mode: 'production',
    module: {
        rules: [
            {
                test: /\.s?css/,
                exclude: '/node_modules/',
                use: [
                    { loader: 'style-loader' },
                    {
                        loader: 'css-loader',
                        options: {
                            sourceMap: false
                        }
                    },
                    {
                        loader: 'sass-loader',
                        options: {
                            sourceMap: false
                        }
                    }
                ]
            }
        ]
    },
  optimization: {
    minimizer: [
      new UglifyJsPlugin({
        exclude: '/node_modules/',
        parallel: 4
      })
    ],
    splitChunks: {
      // chunks: "all"
    }
  }
});
