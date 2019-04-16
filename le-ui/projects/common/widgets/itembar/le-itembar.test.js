import React from "common/react-vendor";
import { shallow, mount } from "enzyme";
import LeItemBar from "./le-itembar";

describe("<LeItemBar /> rendering", () => {
  it("<LeItemBar /> with label and icon", () => {
    const wrapper = mount(
      <LeItemBar
        config={{}}
      />
    );
    expect(wrapper.find(LeItemBar).exists()).toBeTruthy();
  });
});
