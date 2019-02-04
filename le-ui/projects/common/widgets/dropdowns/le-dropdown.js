import React, { Component, react2angular } from "common/react-vendor";
import './le-dropdown.scss';

export class Dropdown extends Component {
    
    constructor(){
        super();

        this.state = {
            displayMenu: false,
        };

        this.showDropdownMenu = this.showDropdownMenu.bind(this);
        this.hideDropdownMenu = this.hideDropdownMenu.bind(this);
    };

    getMenuItems() {
        let menuItems = this.props.menuItems.map((item, index) => {
            return (
                <li key={index}>
                    {item.title}
                </li>
            )
        });
    }

    showDropdownMenu(event) {
        event.preventDefault();
        this.setState({ displayMenu: true }, () => {
            document.addEventListener('click', this.hideDropdownMenu);
        });
    }

    hideDropdownMenu() {
        this.setState({ displayMenu: false }, () => {
            document.removeEventListener('click', this.hideDropdownMenu);
        });
    }

    render() {
        return (
            <div  className="" style = {{background:"red",width:"200px"}} >
                <div className="" onClick={this.showDropdownMenu}>Remodel</div>

                { this.state.displayMenu ? (
                        <ul>
                            {this.getMenuItems()}
                        </ul>
                    ) : (
                        null
                    )
                }
            </div>
        )
    }
}

angular
    .module("le.dropdown", [])
    .component(
        "dropdownComponent",
        react2angular(Dropdown, [], ["$state"])
    );