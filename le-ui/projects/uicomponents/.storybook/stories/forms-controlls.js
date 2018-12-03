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
        clearIcon: boolean('config.clearIcon', true),
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
        icon: text("config.icon", undefined),
        label: text("config.lable", "Lattice Engines"),
        iconside: select('config.iconside', [LEFT, RIGHT], LEFT)
      }}
      callback={action("clicked in the link")}
    />
  </form>
));


stories.add("number field", () => (
  <p>TODO</p>
));

stories.add("date picker", () => (
  <p>TODO</p>
));


stories.add("chip", () => (
  <p>TODO</p>
));


stories.add("dropdown", () => (
  <p>TODO</p>
));


stories.add("radioButton", () => (
  <p>TODO</p>
));


stories.add("checkbox", () => (
  <p>TODO</p>
));


stories.add("switch", () => (
  <p>TODO</p>
));


stories.add("toogleButton", () => (
  <p>TODO</p>
));