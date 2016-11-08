import sys

def generate_options(options):
    lines = []
    for opt in options.split(","):
        lines.append("<option value=\"/%s/v2/api-docs\">%s</option>\n" % (opt, opt))
    return lines

def replace_options(options):
    print options
    new_content = []
    with open("/usr/local/apache2/htdocs/index.html.tpl", 'r') as f:
        for line in f:
            if "{{options}}" in line:
                new_content += generate_options(options)
            else:
                new_content.append(line)

    with open("/usr/local/apache2/htdocs/index.html", 'w') as f:
        for line in new_content:
            f.write(line)

if __name__ == "__main__":
    replace_options(sys.argv[1])