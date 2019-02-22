const webpack = require('webpack');
const merge = require('webpack-merge');
const UglifyJsPlugin = require('uglifyjs-webpack-plugin');

const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const common = require('./webpack.common.js');

module.exports = merge(common, {
    mode: 'production',
    plugins: [
        //new HardSourceWebpackPlugin(),
        new MiniCssExtractPlugin({
            filename: '[name].' + Date.now() + '.css'
        })
    ],
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
