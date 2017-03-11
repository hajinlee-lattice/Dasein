import sys

SPECIAL_CONTEXT_PATH = {
    "scoringapi": "score",
    "matchapi": "match",
    "playmaker": "api"
}

def generate_options(options):
    lines = []
    for opt in options.split(","):
        if opt == 'ui':
            continue
        context_path = opt
        if opt in SPECIAL_CONTEXT_PATH:
            context_path = SPECIAL_CONTEXT_PATH[opt]
        lines.append("<option value=\"/%s/v2/api-docs\">%s</option>\n" % (context_path, opt))
    return lines

def replace_options(options):
    print options
    new_content = []
    with open("/usr/local/apache2/htdocs/index.html.tpl", 'r') as f:
        for line in f:
            if "{{options}}" in line:
                new_content += generate_options(options)
            elif "{{firstOption}}" in line:
                new_content.append(line.replace("{{firstOption}}", options.split(",")[0]))
            else:
                new_content.append(line)

    with open("/usr/local/apache2/htdocs/index.html", 'w') as f:
        for line in new_content:
            f.write(line)

if __name__ == "__main__":
    replace_options(sys.argv[1])