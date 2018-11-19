import React from "../../../common/react-vendor";
import { storiesOf } from "@storybook/react";
import { action } from "@storybook/addon-actions";
import {
  withKnobs,
  text,
  boolean,
  select,
  number
} from "@storybook/addon-knobs";
import "../../../common/assets/css/font-awesome.min.css";
import "../../../common/widgets/layout/layout.scss";

import LeMenu from "../../../common/widgets/menu/le-menu";
import LeMenuItem from "../../../common/widgets/menu/le-menu-item";

const stories = storiesOf("Panels", module);

stories.addDecorator(withKnobs);

stories.add("hpanel", () => (
  <p>TODO</p>
));


stories.add("vpanel", () => (
  <p>TODO</p>
));


