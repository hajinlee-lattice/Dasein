
def get_stack(client, stack_name):
    stacks = client.describe_stacks()['Stacks']
    for stack in stacks:
        if stack['StackName'] == stack_name:
            current_template = client.get_template(StackName=stack_name)
            stack['Template'] = current_template
            return stack
    return None


def log(msg, *args):
    print msg.format(*args)
