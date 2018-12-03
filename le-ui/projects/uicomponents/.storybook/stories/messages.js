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


const stories = storiesOf("Messages", module);

stories.addDecorator(withKnobs);
stories.add("banner", () => (
  <p>TODO</p>
));

stories.add("notice", () => (
  <p>TODO</p>
));

stories.add("toast", () => (
  <p>TODO</p>
));
