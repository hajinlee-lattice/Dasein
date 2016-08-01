#!/usr/local/bin/python

# $LastChangedBy$
# $LastChangedDate$
# $Rev$

import fastavro as avro
import sys

def printavro(avrofilename, nlines):
    with open(avrofilename, mode='rb') as avrofile:
        reader = avro.reader(avrofile)
        schema = reader.schema
        fields = schema['fields']
        formattedline = ''
        for col in fields:
            formattedline = formattedline + '{0:18s}'.format(col['name'])
        print formattedline
        i = 0
        if nlines == 0:
            return
        for row in reader:
            i += 1
            if (nlines > 0) and (i > nlines):
                break
            formattedline = ''
            for col in fields:
                datatype = col['type'][0]
                formattedline = formattedline + '{0:18s}'.format(str(row[col['name']]))
            print formattedline

def usage(cmd, exit_code):
    path = ''
    i = cmd.rfind('\\')
    j = cmd.rfind('/')
    if( i != -1 ):
        path = cmd[:i]
        cmd = cmd[i+1:]
    elif( j != -1 ):
        path = cmd[:j]
        cmd = cmd[j+1:]

    print ''
    print 'PATH: {0}'.format(path)
    print 'Usage: {0} <filename.avro> [lines_to_print]'.format(cmd)
    print ''

    exit(exit_code)

if __name__ == "__main__":

    if len(sys.argv) == 1:
        usage(sys.argv[0], 0)

    if len(sys.argv) > 3:
        usage(sys.argv[0], 1)

    avrofilename = sys.argv[1]
    try:
        nlines = int(sys.argv[2]) if len(sys.argv) == 3 else -1
    except:
        sys.stderr.write('\nError: "lines to print" must be an integer\n')
        usage(sys.argv[0], 2)

    printavro(avrofilename, nlines)
