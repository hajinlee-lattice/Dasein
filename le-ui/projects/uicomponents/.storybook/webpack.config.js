const path = require("path");

module.exports = {
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
      { test: /\.(png|svg|jpg|gif)$/, exclude: /node_modules/, use: ["file-loader"] },
      { test: /\.(woff|woff2|eot|ttf|otf)$/, exclude: /node_modules/, use: ["file-loader"] },
      { test: /\.html$/, exclude: /node_modules/, use: ["raw-loader"] },
      {
        test: /\.js$/,
        exclude: /node_modules/,
        use: { loader: "babel-loader" }
      }
    ]
  }
};
