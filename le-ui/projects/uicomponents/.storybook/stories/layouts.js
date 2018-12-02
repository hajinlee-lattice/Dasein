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

import './layouts.scss';
const stories = storiesOf("Layouts", module);

stories.addDecorator(withKnobs);

stories.add("Complex Layout", () => (
  <div className="container">
    <LeVPanel vstretch={"true"} className="sub-container">
        <LeHPanel hstretch={"true"} className="sub-container header"><span>Toolbar</span></LeHPanel>
        <LeHPanel hstretch={"true"} className="sub-container body"><span>Body</span></LeHPanel>
        <LeHPanel hstretch={"true"} className="sub-container footer"><span>Footer</span></LeHPanel>
    </LeVPanel>
  </div>
));
