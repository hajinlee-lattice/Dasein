import argparse

TABLE_TO_DELETE = []
TABLE_TO_CREATE = []
ALTER_TABLE_LINE = None
ALTER_TABLE_DROP_LINES = []
DROP_FKS = []
CREATE_FKS = []

LINES_TO_FILE = []

def main():
    global LINES_TO_FILE
    args = parse_args()
    diff_file = args.diff
    ddl_file = args.generated
    out_file = args.output
    print "using diff file %s" % diff_file
    print "using ddl file %s" % ddl_file
    print "using output file %s" % out_file

    with open(out_file, 'w') as ofile:
        ofile.write('USE `PLS_MultiTenant`;\n')

    process_diff(diff_file)
    cleanup_script(out_file)

    LINES_TO_FILE=[]
    process_ddl(ddl_file)
    with open(out_file, 'a') as ofile:
        for line in LINES_TO_FILE:
            ofile.write(line)

    with open(out_file + '.2', 'w') as ofile:
        ofile.write('USE `PLS_MultiTenant`;\n')
        for table in TABLE_TO_DELETE:
            ofile.write('DROP TABLE IF EXISTS `%s`;\n' % table)

def process_diff(diff_file):
    flags = {
        'table_missing_on_local': False,
        'table_missing_on_remote': False,
        'alter_table_block': False,
        'alter_table_drop_events': False
    }
    with open(diff_file, 'r') as file:
        for line in file:
            flags['alter_table_drop_events'] = False
            if '# WARNING: Objects in server1' in line:
                flags['table_missing_on_local'] = True
            elif '# WARNING: Objects in server2' in line:
                flags['table_missing_on_remote'] = True
            elif 'ALTER TABLE' in line:
                flags['alter_table_block'] = True
            elif 'DROP' in line:
                flags['alter_table_drop_events'] = True
            flags = process_diff_line(line, flags)


def process_diff_line(line, flags):
    if flags['table_missing_on_local']:
        flags['table_missing_on_local'] = process_missing_table_line(line, False)
    elif flags['table_missing_on_remote']:
        flags['table_missing_on_remote'] = process_missing_table_line(line, True)
    elif flags['alter_table_block']:
        process_alter_table_line(line, flags)
    return flags

def process_alter_table_line(line, flags):
    global ALTER_TABLE_LINE
    global ALTER_TABLE_DROP_LINES
    global DROP_FKS
    global CREATE_FKS
    if 'ALTER TABLE' in line:
        # first line
        ALTER_TABLE_LINE = line
        return

    if flags['alter_table_drop_events']:
        ALTER_TABLE_DROP_LINES.append(line)
        key = find_fk_to_be_drop(line)
        if key is not None:
            print "Find a FK to be dropped: " + line.replace('\n', '').replace('`pls_multitenant`.', '`PLS_MultiTenant`.')
            DROP_FKS.append(key)
        return
    else:
        if ALTER_TABLE_LINE is not None:
            LINES_TO_FILE.append(ALTER_TABLE_LINE)
            if len(ALTER_TABLE_DROP_LINES) > 0:
                last_line = ALTER_TABLE_DROP_LINES[-1]
                last_line = last_line.replace(',', ';')
                ALTER_TABLE_DROP_LINES[-1] = last_line
                for line2 in ALTER_TABLE_DROP_LINES:
                    LINES_TO_FILE.append(line2)
                LINES_TO_FILE.append("\n" + ALTER_TABLE_LINE)
                ALTER_TABLE_DROP_LINES = []
            ALTER_TABLE_LINE = None
        LINES_TO_FILE.append(line)
        key = find_fk_to_be_created(line)
        if key is not None:
            print "Find a FK to be creataed: " + line.replace('\n', '').replace('`pls_multitenant`.', '`PLS_MultiTenant`.')
            CREATE_FKS.append(key)
        if line.strip() == '':
            flags['alter_table_block'] = False

def find_fk_to_be_drop(line):
    if 'DROP FOREIGN KEY' in line:
        key = line.split('DROP FOREIGN KEY')[1].strip()
        key = key.replace(";", "").replace(",", "")
        return key

def find_fk_to_be_created(line):
    if 'ADD CONSTRAINT' in line and 'FOREIGN KEY' in line:
        key = line.split('ADD CONSTRAINT')[1].split('FOREIGN KEY')[0].strip()
        return key

def process_missing_table_line(line, missing_on_remote):
    global TABLE_TO_DELETE
    global TABLE_TO_CREATE
    if '# WARNING' in line:
        # first line int the block
        return True
    elif '#        TABLE' not in line:
        # last line in the block
        return False
    else:
        parts = line.split(':')
        table = parts[1].strip()
        if missing_on_remote:
            print "found a table to be created on remote server: " + table
            TABLE_TO_CREATE.append(table)
        else:
            print "found a table can be deleted from remote server: " + table
            TABLE_TO_DELETE.append(table)
        return True

def cleanup_script(outfile):
    global LINES_TO_FILE

    lines = []
    for line in LINES_TO_FILE:
        if len(line.replace(',', '').strip()) == 0:
            # skip empty line
            continue
        key = find_fk_to_be_drop(line)
        if key is None:
            key = find_fk_to_be_created(line)
        if key is not None:
            # this line is about create or drop foreign key
            if key in DROP_FKS and key in CREATE_FKS:
                # skip drop and create the same fk
                continue
        lines.append(line)

    LINES_TO_FILE = []
    in_alter_table_block = False
    alter_table_block = []
    for line in lines:
        if 'ALTER TABLE' in line:
            # end of previous block
            if len(alter_table_block) > 1:
                for l in alter_table_block:
                    LINES_TO_FILE.append(l)
            # new alter block
            alter_table_block = [ line ]
            in_alter_table_block = True
        elif in_alter_table_block:
            alter_table_block.append(line)
        elif len(line.strip()):
            # end of a block
            if len(alter_table_block) > 1:
                for l in alter_table_block:
                    LINES_TO_FILE.append(l)
            in_alter_table_block = False
            alter_table_block = []

    with open(outfile, 'a') as ofile:
        for line in LINES_TO_FILE:
            ofile.write(line)


def process_ddl(ddl_file):
    global TABLE_TO_CREATE
    global LINES_TO_FILE
    with open(ddl_file, 'r') as file:
        seen_first_create = False
        for line in file:
            if not seen_first_create and ('create table' in line):
                seen_first_create = True
            if not seen_first_create:
                continue
            for table in TABLE_TO_CREATE:
                if ('`%s`' % table) in line:
                    LINES_TO_FILE.append(table)

def parse_args():
    parser = argparse.ArgumentParser(description='Replace tokens in properties')
    parser.add_argument('-d', dest='diff', type=str, required=True, help='schema diff generate by mysqldiff')
    parser.add_argument('-g', dest='generated', type=str, required=True, help='ddl generated by hibernate')
    parser.add_argument('-o', dest='output', type=str, required=True, help='output file')
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()