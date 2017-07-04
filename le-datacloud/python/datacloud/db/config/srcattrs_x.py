from srcattrs import execute

def func(args):
    execute(args.version)

def add_parser(commands):
    description="""
    Genarate SourceAttributes table
    """
    parser = commands.add_parser("srcattrs", description=description)
    parser.add_argument('-v', dest='version', type=str, required=True, help='Based on this version in LDC_ManageDB.AccountMasterColumn')
    parser.set_defaults(func=func)
