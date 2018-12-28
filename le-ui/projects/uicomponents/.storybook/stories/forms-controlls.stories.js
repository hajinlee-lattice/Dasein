import "./form-controlls.scss";
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
import LeInputText from "common/widgets/inputs/le-input-text";
import LeLink, { LEFT, RIGHT } from "common/widgets/link/le-link";
import LeAutocomplete from "common/widgets/autocomplete/le-autocomplete";
import LeSwitch from "common/widgets/switch/le-switch";

const stories = storiesOf("Form Controls", module);

stories.addDecorator(withKnobs);

const defaultDebounce = 2000;
stories.add("text field", () => (
  <form>
    <LeInputText
      config={{
        placeholder: "Callback with debounce",
        icon: text("config.icon", "fa fa-search"),
        label: text("config.lable", "Lattice Engines text field"),
        clearIcon: boolean("config.clearIcon", true),
        debounce: number("config.debounce", defaultDebounce)
      }}
      callback={action("typed in text field")}
    />
  </form>
));

//LINK

stories.add("link", () => (
  <form>
    <LeLink
      config={{
        icon: text("config.icon", "fa fa-info-circle"),
        label: text("config.lable", "Lattice Engines"),
        iconside: select("config.iconside", [LEFT, RIGHT], LEFT)
      }}
      callback={action("clicked in the link")}
    />
  </form>
));

stories.add("number field", () => <p>TODO</p>);

stories.add("date picker", () => <p>TODO</p>);

stories.add("chip", () => (
  <form>
    <LeAutocomplete
      callback={action("chip added")}
      listItems={[
        { id: 1, name: "apples", displayName: "Apples" },
        { id: 2, name: "bananas", displayName: "Bananas" },
        { id: 3, name: "oranges", displayName: "Oranges" },
        { id: 4, name: "pineapples", displayName: "Pineapples" }
      ]}
    />
  </form>
));

stories.add("dropdown", () => <p>TODO</p>);

stories.add("radioButton", () => <p>TODO</p>);

stories.add("checkbox", () => <p>TODO</p>);

stories.add("switch", () => (
  <div className="switch-container">
    <LeSwitch 
    isChecked={false} 
      callback={(state) => {
        console.log('State ', state);
      }}
    />
  </div>
));

stories.add("toogleButton", () => <p>TODO</p>);
