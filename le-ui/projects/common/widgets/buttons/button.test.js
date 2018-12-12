import React from "common/react-vendor";
import { shallow } from "enzyme";
import LeButton, { LEFT, RIGHT } from "./le-button";

describe("<LeButton /> state and colors", () => {
  it("<LeButton /> with no initial state", () => {
    const wrapper = shallow(
      <LeButton
        name="state-button"
        config={{
          label: "Lattice Engines"
        }}
      />
    );
    expect(wrapper.state("disabled")).toBe(false);
  });
  it("<LeButton /> with state disabled", () => {
    const wrapper = shallow(
      <LeButton
        name="state-button"
        config={{
          label: "Lattice Engines"
        }}
        state={{ disabled: true }}
      />
    );
    expect(wrapper.state("disabled")).toBe(true);
  });

  it("renders <LeButton /> component disabled", () => {
    const wrapper = shallow(
      <LeButton
        name="disable-button"
        config={{ label: "Test" }}
        disabled={true}
      />
    );
    expect(wrapper.find("button")).toBeDisabled();
  });

  it("renders <LeButton /> component NOT disabled", () => {
    const wrapper = shallow(
      <LeButton
        name="disable-button"
        config={{ label: "Test" }}
        disabled={false}
      />
    );
    expect(wrapper.find("button")).not.toBeDisabled();
  });

  it("renders <LeButton /> component orange color", () => {
    const wrapper = shallow(
      <LeButton
        name="disable-button"
        config={{ label: "Test", classNames: "button orange-button" }}
      />
    );
    expect(wrapper.find("button")).toHaveClassName("button orange-button");
  });

  it("renders <LeButton /> component blue color", () => {
    const wrapper = shallow(
      <LeButton
        name="disable-button"
        config={{ label: "Test", classNames: "button blue-button" }}
      />
    );
    expect(wrapper.find("button")).toHaveClassName("button blue-button");
  });

  it("renders <LeButton /> component gray color", () => {
    const wrapper = shallow(
      <LeButton
        name="disable-button"
        config={{ label: "Test", classNames: "button gray-button" }}
      />
    );
    expect(wrapper.find("button")).toHaveClassName("button gray-button");
  });

  it("<LeButton /> with only label", () => {
    const wrapper = shallow(
      <LeButton
        name="label-button"
        config={{
          label: "Lattice Engines"
        }}
      />
    );
    expect(wrapper.find("button").children()).toHaveLength(1);
  });
});

describe("<LeButton /> icon", () => {
  it("<LeButton /> with icon on the left", () => {
    const wrapper = shallow(
      <LeButton
        name="icon-button"
        config={{
          label: "Lattice Engines",
          iconside: LEFT,
          icon: "fa fa-search"
        }}
      />
    );
    expect(wrapper.find("button").children()).toHaveLength(2);
  });
  it("<LeButton /> with icon on the right", () => {
    const wrapper = shallow(
      <LeButton
        name="icon-button"
        config={{
          label: "Lattice Engines",
          iconside: RIGHT,
          icon: "fa fa-search"
        }}
      />
    );
    expect(wrapper.find("button").children()).toHaveLength(2);
  });
});

describe("<LeButton/> interactions", () => {
  it("<LeButton /> click ", () => {
    const mockCallBack = jest.fn();

    const button = shallow(
      <LeButton
        name="clickable-button"
        callback={mockCallBack}
        config={{
          label: "Lattice Engines",
          iconside: RIGHT,
          icon: "fa fa-search"
        }}
      />
    );
    button.find("button").simulate("click");
    expect(mockCallBack.mock.calls[0][0]).toBe('clickable-button');
    expect(mockCallBack.mock.calls[0][1]).toEqual({disabled: false});
  });
});
