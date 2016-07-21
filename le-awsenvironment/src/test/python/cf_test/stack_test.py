from latticeengines.cf.module.stack import Stack


def test_template():
    t = Stack("Testing stack")
    assert t.template()["Description"] == "Testing stack"
    assert "Mappings" in t.template()
    assert "Parameters" in t.template()
    assert "Properties" not in t.template()