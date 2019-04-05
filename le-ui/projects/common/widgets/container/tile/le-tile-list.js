import React, { Component } from '../../../react-vendor';
import Aux from '../../hoc/_Aux';
import './le-tile-list.scss';

class LeTileList extends Component {
    constructor(props) {
        super(props);
    }
    render() {
        return (
            <Aux>
                <li className={`${"le-tile-list"} ${this.props.classNames ? this.props.classNames : ''}`}>
                    {this.props.children}
                </li>
            </Aux>
        );
    }
}

export default LeTileList;
