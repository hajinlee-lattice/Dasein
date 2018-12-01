import React from "../../../common/react-vendor";
import { storiesOf } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import {
  withKnobs,
  text,
  boolean,
  select,
  number,
  radios
} from "@storybook/addon-knobs";
import "../../../common/assets/css/font-awesome.min.css";
// import "../../../common/widgets/layout/layout.scss";

import LeVPanel from "../../../common/widgets/container/le-v-panel";
import LeHPanel from "../../../common/widgets/container/le-h-panel";
import LeButton from "../../../common/widgets/buttons/le-button";

const stories = storiesOf("Layouts", module);

stories.addDecorator(withKnobs);

const options = {
  Left: "left",
  Center: "center",
  Right: "right"
};

const vOptions = {
  Top: "top",
  Center: "center",
  Bottom: "bottom"
};

stories.add("Complex Layout", () => (
  <div className="container">
    TODO
  </div>
));
