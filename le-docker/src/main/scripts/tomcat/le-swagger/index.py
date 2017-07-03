import json
import sys

SPECIAL_CONTEXT_PATH = {
    "scoringapi": "score",
    "matchapi": "match",
    "playmaker": "api"
}

def generate_options(options):
    apis = []
    for opt in options.split(","):
        if opt == 'ui':
            continue
        context_path = opt
        if opt in SPECIAL_CONTEXT_PATH:
            context_path = SPECIAL_CONTEXT_PATH[opt]
        apis.append({
            'url': '/%s/v2/api-docs' % context_path,
            'name': opt

        })
    return apis

def replace_options(options):
    print options
    new_content = []
    with open("/usr/local/apache2/htdocs/index.html.tpl", 'r') as f:
        for line in f:
            if "{{apis}}" in line:
                new_content += "  var apis = " + json.dumps(generate_options(options)) + "\n"
            else:
                new_content.append(line)

    with open("/usr/local/apache2/htdocs/index.html", 'w') as f:
        for line in new_content:
            f.write(line)

if __name__ == "__main__":
    replace_options(sys.argv[1])