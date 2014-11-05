import logging
from lxml import etree
from lxml.builder import E, ElementMaker
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import _tree
import traceback

from leframework.codestyle import overrides
from leframework.model.state import State
from pipelinefwk import ModelStep


class PMMLModelGenerator(State):

    def __init__(self):
        State.__init__(self, "PMMLModelGenerator")
        self.logger = logging.getLogger(name = 'pmmlmodelgenerator')
        
    
    @overrides(State)
    def execute(self):
        try:
            self.EPMML = ElementMaker(nsmap={'xsi':'http://www.w3.org/2001/XMLSchema-instance'})
            mediator = self.mediator
            steps = mediator.pipeline.getPipeline()
            
            for step in steps:
                if isinstance(step, ModelStep):
                    rfModel = step.getModel()
                    
                    if isinstance(rfModel, RandomForestClassifier):
                        rfExtract = self.extractRF(rfModel, step.getModelInputColumns())
                        with open(mediator.modelLocalDir + "/rfpmml.xml", "w") as f:
                            f.write(etree.tostring(rfExtract, pretty_print=True))
        except :
            tb = traceback.format_exc()
            self.logger.warning(tb)

    def recursiveExtract(self, tree, nodeID, splitPredicate, modelInputColumns):
        if nodeID == _tree.TREE_LEAF:
            raise ValueError("Invalid node_id %s" % _tree.TREE_LEAF)
    
        left_child = tree.children_left[nodeID]
        right_child = tree.children_right[nodeID]
    
        samples = int(tree.n_node_samples[nodeID])
        featureID = modelInputColumns[int(tree.feature[nodeID])]
        threshold = float(tree.threshold[nodeID])
        value = tree.value[nodeID]

        if left_child != _tree.TREE_LEAF:
            leftSplitPredicate = E.SimplePredicate(field=featureID, operator='lessOrEqual', value=str(threshold))
            left_child = self.recursiveExtract(tree, left_child, leftSplitPredicate, modelInputColumns)
            rightSplitPredicate = E.SimplePredicate(field=featureID, operator='greaterThan', value=str(threshold))
            right_child = self.recursiveExtract(tree, right_child, rightSplitPredicate, modelInputColumns)
        
            curNode = E.Node(splitPredicate,
                             left_child, 
                             right_child,
                             recordCount=str(samples))
        else:
            positive = E.ScoreDistribution(value='1', recordCount=str(value[0,1]))
            negative = E.ScoreDistribution(value='0', recordCount=str(value[0,0]))
            curNode = E.Node(splitPredicate,
                             positive, 
                             negative,
                             recordCount=str(samples))
        return curNode

    def extractDT(self, dtClassifier, modelInputColumns):
        miningFieldList = [E.MiningField(name=x, usageType='active') for x in modelInputColumns]
        miningFieldList.append(E.MiningField(name='P1_Event', usageType='predicted'))
        miningSchema = E.MiningSchema(*miningFieldList)

        tree = dtClassifier.tree_
        extractedTree = self.recursiveExtract(tree, 0, E.True(), modelInputColumns)
    
        treeModel = E.TreeModel(miningSchema, extractedTree, functionName= 'classification', splitCharacteristic='binarySplit')
        return treeModel
    
    
    def extractRF(self, rfClassifier, modelInputColumns):
        dataDictionaryList = [E.DataField(name=x, optype='continuous', dataType='double') for x in modelInputColumns]
        segmentList = [E.Segment(E.True(), self.extractDT(x, modelInputColumns)) for x in rfClassifier.estimators_]
        miningFieldList = [E.MiningField(name=x, usageType='active') for x in modelInputColumns]
        miningFieldList.append(E.MiningField(name='P1_Event', usageType='predicted'))
        miningSchema = E.MiningSchema(*miningFieldList)
        segmentation = E.Segmentation(*segmentList, multipleModelMethod='average')
        miningModel = E.MiningModel(miningSchema, segmentation)    
        pmml = self.EPMML.PMML(E.Header('', description='PLS Random Forest Model'),
                          E.DataDictionary(*dataDictionaryList, numberOfFields='%d' % len(modelInputColumns)),
                          E.MiningModel(*miningModel, modelName='randomForestModel', functionName='classification'),
                          xmlns="http://www.dmg.org/PMML-4_0",
                          version='4.2')

        return pmml
