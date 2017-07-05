import amattrs_x
import amcols_x
import srcattrs_x


def add_parser(commands):
    description="Tools for LDC_ConfigDB"
    parser = commands.add_parser("configdb", description=description)
    commands = parser.add_subparsers(help="pick your sub-command. put sub-command before -h will give your more info.")

    # add parsers
    amattrs_x.add_parser(commands)
    srcattrs_x.add_parser(commands)
    amcols_x.add_parser(commands)
