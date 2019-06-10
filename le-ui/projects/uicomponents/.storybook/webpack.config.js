const path = require("path");

module.exports = {
  resolve: {
    alias: {
      common: path.resolve(__dirname, "../../common"),
      app: path.resolve(__dirname, "../../common/app"),
      components: path.resolve(__dirname, "../../common/components"),
      widgets: path.resolve(__dirname, "../../common/widgets"),
      atlas: path.resolve(__dirname, "../../atlas/app"),
      assets: path.resolve(__dirname, "assets")
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
              sourceMap: false
            }
          }
        ]
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
        test: /\.js$/,
        exclude: /node_modules/,
        use: { loader: "babel-loader", options:{
          presets: [
              "@babel/preset-env",
              "@babel/preset-react"
          ],
          plugins: [
              "@babel/plugin-transform-spread",
              "@babel/plugin-syntax-dynamic-import",
              "@babel/plugin-syntax-import-meta",
              "@babel/plugin-proposal-class-properties",
              "@babel/plugin-proposal-json-strings",
              [
                  "@babel/plugin-proposal-decorators",
                  {
                      "legacy": true
                  }
              ],
              "@babel/plugin-proposal-function-sent",
              "@babel/plugin-proposal-export-namespace-from",
              "@babel/plugin-proposal-numeric-separator",
              "@babel/plugin-proposal-throw-expressions"
          ]
          }  }
      }
    ]
  }
};
