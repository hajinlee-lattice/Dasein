import React from "common/react-vendor";
import { mount } from "enzyme";
import LeInputText from "./le-input-text";

describe("<LeInputText /> rendering", () => {
  it("<LeInputText /> full options", () => {
    const wrapper = mount(
      <LeInputText
        config={{
          placeholder: "Callback with debounce",
          icon: "fa fa-search",
          label: "Lattice Engines text field",
          clearIcon: true,
          debounce: 2000
        }}
      />
    );

    let wrapperDom = wrapper.find(LeInputText);
    expect(wrapperDom.exists()).toBeTruthy();
    expect(wrapperDom.text()).toContain("Lattice Engines text field");
    let children = wrapperDom.children();
    expect(children.find('input').exists()).toBeTruthy();
    expect(children.find('.fa-search').exists()).toBeTruthy();
    wrapperDom.find('input').simulate('change', {target: {value: 'Test value'}});
    expect( wrapperDom.find('input').getDOMNode().value).toContain("Test value");
    
  });

  it("<LeInputText /> only input", () => {
    const wrapper = mount(
      <LeInputText
        config={{
          placeholder: "Callback with debounce",
          clearIcon: true,
          debounce: 2000
        }}
      />
    );

    let wrapperDom = wrapper.find(LeInputText);
    expect(wrapperDom.exists()).toBeTruthy();
    expect(wrapperDom.text()).not.toContain("Lattice Engines text field");
    let children = wrapperDom.children();
    expect(children.find('input').exists()).toBeTruthy();
    expect(children.find('.fa-search').exists()).toBeFalsy();
    wrapperDom.find('input').simulate('change', {target: {value: 'Test value'}});
    expect( wrapperDom.find('input').getDOMNode().value).toContain("Test value");
  });

  it("<LeInputText /> only input and label", () => {
    const wrapper = mount(
      <LeInputText
        config={{
          placeholder: "Callback with debounce",
          label: "Lattice Engines text field",
          clearIcon: true,
          debounce: 2000
        }}
      />
    );

    let wrapperDom = wrapper.find(LeInputText);
    expect(wrapperDom.exists()).toBeTruthy();
    expect(wrapperDom.text()).toContain("Lattice Engines text field");
    let children = wrapperDom.children();
    expect(children.find('input').exists()).toBeTruthy();
    expect(children.find('.fa-search').exists()).toBeFalsy();
    wrapperDom.find('input').simulate('change', {target: {value: 'Test value'}});
    expect( wrapperDom.find('input').getDOMNode().value).toContain("Test value");
  });

  it("<LeInputText /> only input and icon", () => {
    const wrapper = mount(
      <LeInputText
        config={{
          placeholder: "Callback with debounce",
          icon: "fa fa-search",
          clearIcon: true,
          debounce: 2000
        }}
      />
    );

    let wrapperDom = wrapper.find(LeInputText);
    expect(wrapperDom.exists()).toBeTruthy();
    expect(wrapperDom.text()).not.toContain("Lattice Engines text field");
    let children = wrapperDom.children();
    expect(children.find('input').exists()).toBeTruthy();
    expect(children.find('.fa-search').exists()).toBeTruthy();
    wrapperDom.find('input').simulate('change', {target: {value: 'Test value'}});
    expect( wrapperDom.find('input').getDOMNode().value).toContain("Test value");
  });
});
