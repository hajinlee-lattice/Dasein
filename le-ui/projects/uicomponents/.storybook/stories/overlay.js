import React from "common/react-vendor";
import { storiesOf } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import {
  withKnobs,
  text,
  boolean,
  select,
  number
} from "@storybook/addon-knobs";
import "common/assets/css/font-awesome.min.css";
import "common/widgets/layout/layout.scss";


const stories = storiesOf("Overlay", module);

stories.addDecorator(withKnobs);

stories.add("tooltip", () => (
  <p>TODO</p>
));
stories.add("modal", () => (
  <p>TODO</p>
));

