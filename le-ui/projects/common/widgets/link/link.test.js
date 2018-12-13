import React from "common/react-vendor";
import { shallow, mount } from "enzyme";
import LeLink, {RIGHT, LEFT} from "./le-link";

describe("<LeLink /> rendering", () => {
  it("<LeLink /> with label and icon", () => {
    const wrapper = mount(
      <LeLink
        config={{
          label: "Lattice Engines",
          iconside: RIGHT,
          icon: "fa fa-info-circle"
        }}
      />
    );
    expect(wrapper.find(LeLink).exists()).toBeTruthy();
    expect(wrapper.find(LeLink).text()).toContain("Lattice Engines");
    expect(wrapper.find('.fa-info-circle').exists()).toBeTruthy();
  });
});

describe("<LeLink/> interactions", () => {
  it("<LeLink /> click ", () => {
    const mockCallBack = jest.fn();

    const wrapper = mount(
      <LeLink
        callback={mockCallBack}
        config={{
          label: "Lattice Engines",
          iconside: RIGHT,
          icon: "fa fa-search"
        }}
      />
    );
    wrapper.find(LeLink).simulate("click");
    expect(mockCallBack.mock.calls.length).toEqual(1);
  });
});
