import React from "common/react-vendor";
import { shallow, mount } from "enzyme";
import LeButtons from "./le-views";

describe("<LeButtons /> rendering", () => {
  it("<LeButtons /> with label and icon", () => {
    const wrapper = mount(
      <LeButtons
        config={{}}
      />
    );
    expect(wrapper.find(LeButtons).exists()).toBeTruthy();
  });
});
