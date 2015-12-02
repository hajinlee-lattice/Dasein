import logging
from lxml import etree
from lxml.builder import E, ElementMaker
from sklearn.tree import _tree
from sklearn.ensemble import RandomForestClassifier
import traceback

from leframework.codestyle import overrides
from leframework.model.state import State
from aggregatedmodel import AggregatedModel
from pipelinefwk import ModelStep
 
 
class PMMLModelGenerator(State):
 
    def __init__(self):
        State.__init__(self, "PMMLModelGenerator")
        self.logger = logging.getLogger(name = 'pmmlmodelgenerator')
        self.EPMML = ElementMaker(nsmap={'xsi':'http://www.w3.org/2001/XMLSchema-instance'})
 
    @overrides(State)
    def execute(self):
        try:
            mediator = self.mediator
            steps = mediator.pipeline.getPipeline()
             
            for step in steps:
                if isinstance(step, ModelStep):
                    model = step.getModel()
                    if isinstance(model, AggregatedModel):
                        pmmlModelExtract = self.extractAggregateModel(model, step.getModelInputColumns())
                    elif isinstance(model, RandomForestClassifier):
                        pmmlModelExtract = self.extractRF(model, step.getModelInputColumns(), False)

                    with open(mediator.modelLocalDir + "/rfpmml.xml", "w") as f:
                        f.write(etree.tostring(pmmlModelExtract, pretty_print=True))
        except :
            tb = traceback.format_exc()
            self.logger.warning(tb)

     # builds a PMML file of the aggregate model from all available PMML files in current directory (.xml extension)
    def extractAggregateModel(self, aggregateModel, modelInputColumns):
        dataDictionaryList = [E.DataField(name=x, optype='continuous', dataType='double') for x in modelInputColumns]
        segmentList = [E.Segment(E.True(), self.extractRF(x, modelInputColumns, True)) for x in aggregateModel.models]
        miningFieldList = [E.MiningField(name=x, usageType='active') for x in modelInputColumns]
        miningFieldList.append(E.MiningField(name='P1_Event', usageType='predicted'))
        miningSchema = E.MiningSchema(*miningFieldList)
        segmentation = E.Segmentation(*segmentList, multipleModelMethod='average')
        miningModel = E.MiningModel(miningSchema, segmentation)    
        pmml = self.EPMML.PMML(E.Header('', description='PLS Aggregated Model'),
                          E.DataDictionary(*dataDictionaryList, numberOfFields='%d' % len(modelInputColumns)),
                          E.MiningModel(*miningModel, modelName='aggregatedModel', functionName='regression'),
                          xmlns="http://www.dmg.org/PMML-4_0",
                          version='4.2')
        return pmml
    
    def recursiveExtract(self, tree, nodeID, splitPredicate, modelInputColumns, isAggregatedModel):
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
            left_child = self.recursiveExtract(tree, left_child, leftSplitPredicate, modelInputColumns, isAggregatedModel)
            rightSplitPredicate = E.SimplePredicate(field=featureID, operator='greaterThan', value=str(threshold))
            right_child = self.recursiveExtract(tree, right_child, rightSplitPredicate, modelInputColumns, isAggregatedModel)
         
            curNode = E.Node(splitPredicate,
                             left_child, 
                             right_child,
                             recordCount=str(samples))
        else:
            positiveReadCount = 0.0
            if(len(value[0]) > 1):
                positiveReadCount = value[0,1]
                
            if isAggregatedModel:
                totalReadCount = value[0,0] + positiveReadCount
                predictedScore = str(positiveReadCount / totalReadCount)
                curNode = E.Node(splitPredicate, score = str(predictedScore), recordCount = str(int(totalReadCount)), positiveReadCount = str(int(positiveReadCount)))
            else:
                negative = E.ScoreDistribution(value='0', recordCount=str(value[0,0]))                  
                positive = E.ScoreDistribution(value='1', recordCount=str(positiveReadCount))


                curNode = E.Node(splitPredicate,
                                 positive, 
                                 negative,
                                 recordCount=str(samples))
        return curNode
 
    def extractDT(self, dtClassifier, modelInputColumns, isAggregatedModel):
        miningFieldList = [E.MiningField(name=x, usageType='active') for x in modelInputColumns]
        miningFieldList.append(E.MiningField(name='P1_Event', usageType='predicted'))
        miningSchema = E.MiningSchema(*miningFieldList)
 
        tree = dtClassifier.tree_
        extractedTree = self.recursiveExtract(tree, 0, E.True(), modelInputColumns, isAggregatedModel)
     
        treeType = 'regression' if isAggregatedModel == True else 'classification'
        treeModel = E.TreeModel(miningSchema, extractedTree, functionName= treeType, splitCharacteristic='binarySplit')
        return treeModel
 
    def extractRF(self, rfClassifier, modelInputColumns, isAggregatedModel):
        segmentList = [E.Segment(E.True(), self.extractDT(x, modelInputColumns, isAggregatedModel)) for x in rfClassifier.estimators_]
        miningFieldList = [E.MiningField(name=x, usageType='active') for x in modelInputColumns]
        miningFieldList.append(E.MiningField(name='P1_Event', usageType='predicted'))
        miningSchema = E.MiningSchema(*miningFieldList)
        segmentation = E.Segmentation(*segmentList, multipleModelMethod='average')
        miningModel = E.MiningModel(miningSchema, segmentation) 
        
        if isAggregatedModel:
            pmml = self.EPMML.PMML(E.MiningModel(*miningModel, modelName='RFModel', functionName='regression'),
                                   xmlns="http://www.dmg.org/PMML-4_0")
            return pmml.getchildren()[0]
        else:
            dataDictionaryList = [E.DataField(name=x, optype='continuous', dataType='double') for x in modelInputColumns]
            pmml = self.EPMML.PMML(E.Header('', description='PLS Random Forest Model'),
                                  E.DataDictionary(*dataDictionaryList, numberOfFields='%d' % len(modelInputColumns)),
                                  E.MiningModel(*miningModel, modelName='randomForestModel', functionName='classification'),
                                  xmlns="http://www.dmg.org/PMML-4_0",
                                  version='4.2')
            return pmml
