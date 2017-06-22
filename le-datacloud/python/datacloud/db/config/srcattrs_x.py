from srcattrs import execute

def func(args):
    execute()

def add_parser(commands):
    description="""
    Genarate SourceAttributes table
    """
    parser = commands.add_parser("srcattrs", description=description)
    parser.set_defaults(func=func)
