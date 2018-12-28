import React, { Component } from "common/react-vendor";
import "./le-chip.scss";
import search from "./le-search.utils";

class LeChip extends Component {
  constructor(props) {
    super(props);
    this.removeChip = this.removeChip.bind(this);
    this.addChip = this.addChip.bind(this);
    this.keyPressedHandler = this.keyPressedHandler.bind(this);
    this.removeChip = this.removeChip.bind(this);
    this.getItem = this.getItem.bind(this);
    this.getChipsSelected = this.getChipsSelected.bind(this);

    this.itemsAvailable = [];
    this.state = {
      value: "",
      chipsSelected: [],
      idSelected: {},
      show: false,
      selected: 0
    };
  }

  removeChip(item, position) {
    let tmpChips = [...this.state.chipsSelected];
    tmpChips.splice(position, 1);
    const idsCopy = { ...this.state.idSelected };
    delete idsCopy[item.id];
    this.setState({ chipsSelected: tmpChips, idSelected: idsCopy });
  }

  getItem(item, index) {
    return (
      <li
        key={index}
        className={`${"le-chips-list-item"} ${
          this.state.selected == index ? "selected" : ""
        }`}
        onClick={event => {
          //.log("Clicked");
          this.addChip(item, index);
        }}
      >
        {item.displayName}
      </li>
    );
  }

  getListItems() {
    let tmp = search(this.props.listItems, this.state.value, "displayName");
    let retUI = [];
    this.itemsAvailable = [];
    let id = 0;
    tmp.forEach((element, index) => {
      if (!this.state.idSelected[element.id]) {
        this.itemsAvailable.push(element);
        retUI.push(this.getItem(element, id));
        id++;
      }
    });
    return retUI;
  }
  keyPressedHandler(event) {
    event.stopPropagation();
    switch (event.keyCode) {
      case 40:
        //ArrowDown
        if (this.state.selected + 1 < this.itemsAvailable.length) {
          this.setState({ selected: this.state.selected + 1 });
        }
        break;
      case 38:
        //ArrowUp
        if (this.state.selected - 1 >= 0) {
          this.setState({ selected: this.state.selected - 1 });
        }
        break;
      case 27:
        //Escape
        this.setState({ show: false });
        break;

      case 8:
        //Backspace
        if (this.state.value == "") {
          this.setState({ show: false });
        }
      case 32:
        //Space with ctrl
        if (event.ctrlKey) {
          this.setState({ selected: 0, show: true });
        }
        break;

      case 13:
        let item = this.itemsAvailable[this.state.selected];
        let indexItem = this.state.selected;
        this.addChip(item, indexItem);
        this.setState({ selected: 0, show: false });
        break;
    }
  }
  searchItems(event) {
    let val = event.target.value;
    this.setState({ value: val, show: true });
  }

  addChip(item, index) {
    let tmpChips = [...this.state.chipsSelected];
    tmpChips.push(item);
    const idsCopy = { ...this.state.idSelected };
    idsCopy[item.id] = item.name;

    this.setState({
      chipsSelected: tmpChips,
      idSelected: idsCopy,
      selected: 0
    });
  }

  getChipsSelected() {
    let chipsUI = this.state.chipsSelected.map((item, index) => {
      return (
        <li key={index} className="le-chip">
          <span className="le-chip-name">{item.displayName}</span>
          <i
            className="le-chip-remove fa fa-times"
            aria-hidden="true"
            onClick={() => {
              this.removeChip(item, index);
            }}
          />
        </li>
      );
    });
    return chipsUI;
  }

  render() {
    return (
      <div className="le-chips">
        <ul className="le-chips-selected">{this.getChipsSelected()}</ul>
        <div className="le-chip-input">
          <i className="fa fa-search" aria-hidden="true" />
          <input
            type="text"
            ref={input => {
              this.chipsInput = input;
            }}
            value={this.state.value}
            onKeyUp={event => {
              this.keyPressedHandler(event);
            }}
            onChange={event => {
              this.searchItems(event);
            }}
            onBlur={event => {
              setTimeout(() => {
                this.setState({ show: false });
              }, 200);
            }}
          />
          <div
            className={`${"le-chip-dropdown"} ${
              this.state.show != "" ? "le-chip-dropdown-show" : ""
            }`}
          >
            <ul
              className={`${"le-chip-dropdown-content"} ${
                this.state.show != "" ? "le-chip-dropdown-show" : ""
              }`}
            >
              {this.getListItems()}
            </ul>
          </div>
        </div>
      </div>
    );
  }
}

export default LeChip;
