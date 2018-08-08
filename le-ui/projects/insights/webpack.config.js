const path = require("path");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const CleanWebpackPlugin = require("clean-webpack-plugin");
const webpack = require("webpack");
const UglifyJsPlugin = require('uglifyjs-webpack-plugin');
module.exports = {
  entry: {
    angular: "../common/angular-vendor.index.js",
    vendor: "../common/vendor.index.js",
    insights: "./index.js"
  },
  devtool: "eval-source-map",
  plugins: [
    new CleanWebpackPlugin(["dist"]),
    new HtmlWebpackPlugin({
      filename: "indexwp.html",
      template: __dirname + "/indexwp.html"
    })
  ],
  
  output: {
    filename: "[name].bundle.js",
    path: path.resolve(__dirname, "dist")
  },
  optimization: {
    minimizer: [
      new UglifyJsPlugin({
        exclude: /\/node_modules/,
        parallel: 4})
    ],
    splitChunks: {
      // chunks: "all"
    }
  },
  module: {
    rules: [
      {
        test: /\.s?css/,
        exclude: /node_modules/,
        use: ["style-loader", "css-loader", "sass-loader"]
      },
      {
        test: /\.(png|svg|jpg|gif)$/,
        exclude: /node_modules/,
        use: ["file-loader"]
      },
      {
        test: /\.(woff|woff2|eot|ttf|otf)$/,
        exclude: /node_modules/,
        use: ["file-loader"]
      },
      { test: /\.html$/, exclude: /node_modules/, use: ["raw-loader"] },
      {
        test: /\.*\.js$/,
        exclude: /node_modules/,
        use: ["ng-annotate-loader"]
      },
      {
        test: /\.js$/,
        exclude: /node_modules/,
        use: { loader: "babel-loader" }
      }
    ]
  },
  watchOptions: {
    aggregateTimeout: 300,
    ignored: /node_modules/
  }
};
