import './breadcrumbs.scss';
import React, { Component } from 'react';

export default class Breadcrumbs extends Component {
    constructor(props) {
        super(props);

        this.state = {
            breadcrumbs: [
                {
                    name: 'files',
                    label: 'Account Data'
                }
            ]
        };
    }

    componentDidMount() {
        let breadcrumbs = this.props.breadcrumbs;
        this.setState({
            breadcrumbs: breadcrumbs
        });
    }

    render() {
        let crumbsArray = this.state.breadcrumbs;
        let crumbs = crumbsArray.map(function(crumb){
                            return <li>{crumb.label}</li>;
                        });
        return (
            <ol>
                {crumbs}
            </ol>
        )
    }
}