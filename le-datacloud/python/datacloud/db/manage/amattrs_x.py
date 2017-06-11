from amattrs import execute

def func(args):
    execute()

def add_parser(commands):
    description="""
    Read metadata from imported spread sheet and generate AccountMaster_Attributes table
    """
    parser = commands.add_parser("amattrs", description=description)
    parser.set_defaults(func=func)
