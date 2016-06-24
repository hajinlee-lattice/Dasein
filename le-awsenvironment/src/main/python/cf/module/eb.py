from .resource import Resource
from .template import Template

class EbApplication(Resource):
    def __init__(self, logicalId):
        Resource.__init__(self, logicalId)
        self._template = {
            "Type": "AWS::ElasticBeanstalk::Application"
        }

class EbApplicationVersion(Resource):
    def __init__(self, logicalId, application, source_bundle):
        assert isinstance(application, EbApplication)
        assert isinstance(source_bundle, EbSourceBundle)

        Resource.__init__(self, logicalId)
        self._template = {
            "Type": "AWS::ElasticBeanstalk::ApplicationVersion",
            "Properties": {
                "ApplicationName": application.ref(),
                "SourceBundle": source_bundle.template()
            }
        }

class EbSourceBundle(Template):
    def __init__(self, bucket, key):
        Template.__init__(self)
        self._template = {
            "S3Bucket": bucket,
            "S3Key": key
        }

class EbConfigurationTemplate(Resource):
    def __init__(self, logicalId, application, solution_name, platform="multi-docker"):
        assert isinstance(application, EbApplication)
        Resource.__init__(self, logicalId)
        self._template = {
            "Type": "AWS::ElasticBeanstalk::ConfigurationTemplate",
            "Properties": {
                "ApplicationName": application.ref(),
                "OptionSettings": [
                    {
                        "Namespace": "aws:autoscaling:asg",
                        "OptionName": "MinSize",
                        "Value": "2"
                    },
                    {
                        "Namespace": "aws:autoscaling:asg",
                        "OptionName": "MaxSize",
                        "Value": "6"
                    },
                    {
                        "Namespace": "aws:elasticbeanstalk:environment",
                        "OptionName": "EnvironmentType",
                        "Value": "LoadBalanced"
                    },
                    {
                        "Namespace": "aws:autoscaling:launchconfiguration",
                        "OptionName": "EC2KeyName",
                        "Value": { "Ref" : "KeyName" }
                    },
                    {
                        "Namespace": "aws:autoscaling:launchconfiguration",
                        "OptionName": "InstanceType",
                        "Value": { "Ref": "InstanceType" }
                    },
                    {
                        "Namespace": "aws:autoscaling:launchconfiguration",
                        "OptionName": "SecurityGroups",
                        "Value": { "Ref": "SecurityGroupId" }
                    },
                    {
                        "Namespace": "aws:ec2:vpc",
                        "OptionName": "VPCId",
                        "Value": { "Ref" : "VpcId" }
                    },
                    {
                        "Namespace": "aws:ec2:vpc",
                        "OptionName": "Subnets",
                        "Value": { "Fn::Join" : [ ",", [
                            { "Ref" : "SubnetId1" },
                            {"Ref": "SubnetId2"}
                        ] ] }
                    },
                    {
                        "Namespace": "aws:ec2:vpc",
                        "OptionName": "ELBSubnets",
                        "Value": { "Fn::Join" : [ ",", [
                            { "Ref" : "SubnetId1" },
                            {"Ref": "SubnetId2"}
                        ] ] }
                    }
                ],
                "SolutionStackName": self._get_solution_stack_name(platform)
            }
        }

    def _get_solution_stack_name(self, platform):
        platform_soluciton_map = {
            "multi-docker": "64bit Amazon Linux 2016.03 v2.1.1 running Multi-container Docker 1.9.1 (Generic)"
        }
        return platform_soluciton_map[platform]

class EbEnvironment(Resource):
    def __init__(self, logicalId, application, application_version, config_template):
        assert isinstance(application, EbApplication)
        assert isinstance(application_version, EbApplicationVersion)
        assert isinstance(config_template, EbConfigurationTemplate)

        Resource.__init__(self, logicalId)
        self._template = {
            "Type": "AWS::ElasticBeanstalk::Environment",
            "Properties": {
                "ApplicationName": application.ref(),
                "TemplateName": config_template.ref(),
                "VersionLabel": application_version.ref()
            }
        }
