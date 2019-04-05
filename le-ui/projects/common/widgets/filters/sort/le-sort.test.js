import React from "common/react-vendor";
import { shallow, mount } from "enzyme";
import LeSort from "./le-sort";

describe("<LeSort /> rendering", () => {
  it("<LeSort /> with label and icon", () => {
    const wrapper = mount(
      <LeSort
        config={{}}
      />
    );
    expect(wrapper.find(LeSort).exists()).toBeTruthy();
  });
});
