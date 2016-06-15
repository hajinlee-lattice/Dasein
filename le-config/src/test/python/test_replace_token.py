import os
import shutil
from replace_token import replace

RESOURCES=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'resources')
BAK_SUFFIX=".bak"

def test_replace():
    conf_dir = os.path.join(RESOURCES, 'conf')
    profile = os.path.join(RESOURCES, 'test.profile')

    if os.path.isdir(conf_dir + BAK_SUFFIX):
        shutil.rmtree(conf_dir + BAK_SUFFIX)
    shutil.copytree(conf_dir, conf_dir + BAK_SUFFIX)

    replace(conf_dir, profile)
    with open(conf_dir + "/app.properties") as file:
        print file.read()
        for line in file:
            if 'app.conf.1' in line:
                assert '=value1' in line
            elif 'app.conf.2' in line:
                assert '=token' in line

    shutil.rmtree(conf_dir)
    shutil.copytree(conf_dir + BAK_SUFFIX, conf_dir)
    shutil.rmtree(conf_dir + BAK_SUFFIX)