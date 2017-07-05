from amcols import execute

def func(args):
    execute()

def add_parser(commands):
    description="""
    Construct AccountMasterColumn table
    """
    parser = commands.add_parser("amcols", description=description)
    parser.set_defaults(func=func)
