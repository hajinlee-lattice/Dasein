import React from "common/react-vendor";
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
import "common/assets/css/font-awesome.min.css";
// import "../../../common/widgets/layout/layout.scss";

import LeVPanel from "common/widgets/container/le-v-panel";
import LeHPanel from "common/widgets/container/le-h-panel";
import LeButton from "common/widgets/buttons/le-button";

import "./layouts.scss";
import LeInputText from "common/widgets/inputs/le-input-text";
const stories = storiesOf("Layouts", module);

stories.addDecorator(withKnobs);

stories.add("Complex Layout", () => (
  <div className="container">
    <LeVPanel vstretch={"true"} hstretch={"true"}>
      <LeHPanel hstretch={"true"} className="header">
        <span>Toolbar</span>
      </LeHPanel>
      <LeHPanel hstretch={"true"} className="body" flex={"10"}>
        <LeVPanel vstretch={true} className={"nav"}>
          <form>
            <LeInputText
              config={{
                placeholder: "Callback with debounce",
                icon: text("config.icon", "fa fa-search"),
                
                clearIcon: boolean("config.clearIcon", true),
                debounce: number("config.debounce", 2000)
              }}
              callback={action("typed in text field")}
            />
          </form>
        </LeVPanel>
        <LeVPanel vstretch={true} className={"main"} flex={"6"}>
          <span>2</span>
        </LeVPanel>
        <LeVPanel vstretch={true} className={"info"}>
          <span>3</span>
        </LeVPanel>
      </LeHPanel>
      <LeHPanel hstretch={"true"} className="footer">
        <span>Footer</span>
      </LeHPanel>
    </LeVPanel>
  </div>
));
