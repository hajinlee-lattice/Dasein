const webpack = require("webpack");
const merge = require("webpack-merge");
const path = require("path");
const common = require("./webpack.common.js");

module.exports = merge(common, {
  mode: "development",
  devtool: "module-source-map",
  output: {
    devtoolModuleFilenameTemplate(info) {
      return `file:///${info.absoluteResourcePath.replace(/\\/g, '/')}`;
    }
  },
  module: {
    rules: [
      {
        test: /\.s?css/,
        exclude: /node_modules/,
        use: [
          { loader: "style-loader" },
          {
            loader: "css-loader",
            options: {
              sourceMap: false
            }
          },
          {
            loader: "sass-loader",
            options: {
              sourceMap: true
            }
          }
        ]
      }
    ]
  },
  watchOptions: {
    aggregateTimeout: 300,
    ignored: /node_modules/
  }
});
