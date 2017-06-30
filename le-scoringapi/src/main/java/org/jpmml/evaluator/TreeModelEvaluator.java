/*
 * Copyright (c) 2013 Villu Ruusmann
 *
 * This file is part of JPMML-Evaluator
 *
 * JPMML-Evaluator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JPMML-Evaluator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with JPMML-Evaluator.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.jpmml.evaluator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import org.dmg.pmml.CompoundPredicate;
import org.dmg.pmml.DataType;
import org.dmg.pmml.EmbeddedModel;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MissingValueStrategyType;
import org.dmg.pmml.NoTrueChildStrategyType;
import org.dmg.pmml.Node;
import org.dmg.pmml.PMML;
import org.dmg.pmml.Predicate;
import org.dmg.pmml.ScoreDistribution;
import org.dmg.pmml.TreeModel;

public class TreeModelEvaluator extends ModelEvaluator<TreeModel> implements HasEntityRegistry<Node> {

    transient
    private BiMap<String, Node> entityRegistry = null;


    public TreeModelEvaluator(PMML pmml){
        super(pmml, TreeModel.class);
    }

    public TreeModelEvaluator(PMML pmml, TreeModel treeModel){
        super(pmml, treeModel);
    }

    @Override
    public String getSummary(){
        return "Tree model";
    }

    @Override
    public BiMap<String, Node> getEntityRegistry(){

        if(this.entityRegistry == null){
            this.entityRegistry = getValue(TreeModelEvaluator.entityCache);
        }

        return this.entityRegistry;
    }

    @Override
    public Map<FieldName, ?> evaluate(ModelEvaluationContext context){
        TreeModel treeModel = getModel();
        if(!treeModel.isScorable()){
            throw new InvalidResultException(treeModel);
        }

        Map<FieldName, ?> predictions;

        MiningFunctionType miningFunction = treeModel.getFunctionName();
        switch(miningFunction){
            case REGRESSION:
                predictions = evaluateRegression(context);
                break;
            case CLASSIFICATION:
                predictions = evaluateClassification(context);
                break;
            default:
                throw new UnsupportedFeatureException(treeModel, miningFunction);
        }

        return OutputUtil.evaluate(predictions, context);
    }

    private Map<FieldName, ?> evaluateRegression(ModelEvaluationContext context){
        Trail trail = new Trail();

        Node node = evaluateTree(trail, context);
        if(node == null){
            return TargetUtil.evaluateRegressionDefault(context);
        }

        Double score = (Double)TypeUtil.parseOrCast(DataType.DOUBLE, node.getScore());

        FieldName targetField = getTargetField();

        NodeScore nodeScore = createNodeScore(node, TargetUtil.evaluateRegressionInternal(targetField, score, context));

        return Collections.singletonMap(targetField, nodeScore);
    }

    private Map<FieldName, ? extends Classification> evaluateClassification(ModelEvaluationContext context){
        TreeModel treeModel = getModel();

        Trail trail = new Trail();

        Node node = evaluateTree(trail, context);
        if(node == null){
            return TargetUtil.evaluateClassificationDefault(context);
        }

        double missingValuePenalty = 1d;

        int missingLevels = trail.getMissingLevels();
        if(missingLevels > 0){
            missingValuePenalty = Math.pow(treeModel.getMissingValuePenalty(), missingLevels);
        }

        NodeScoreDistribution result = createNodeScoreDistribution(node, missingValuePenalty);

        return TargetUtil.evaluateClassification(result, context);
    }

    private Node evaluateTree(Trail trail, EvaluationContext context){
        TreeModel treeModel = getModel();

        Node root = treeModel.getNode();
        if(root == null){
            throw new InvalidFeatureException(treeModel);
        }

        Boolean status = evaluateNode(trail, root, context);

        if(status != null && status.booleanValue()){
            trail = handleTrue(trail, root, context);

            Node node = trail.getResult();

            // "It is not possible that the scoring process ends in a Node which does not have a score attribute"
//            if(node != null && !node.hasScore()){
//                throw new InvalidFeatureException(node);
//            }

            return node;
        }

        return null;
    }

    private Boolean evaluateNode(Trail trail, Node node, EvaluationContext context){
        EmbeddedModel embeddedModel = node.getEmbeddedModel();
        if(embeddedModel != null){
            throw new UnsupportedFeatureException(embeddedModel);
        }

        Predicate predicate = node.getPredicate();
        if(predicate == null){
            throw new InvalidFeatureException(node);
        } // End if

        // A compound predicate whose boolean operator is "surrogate" represents a special case
        if(predicate instanceof CompoundPredicate){
            CompoundPredicate compoundPredicate = (CompoundPredicate)predicate;

            PredicateUtil.CompoundPredicateResult result = PredicateUtil.evaluateCompoundPredicateInternal(compoundPredicate, context);
            if(result.isAlternative()){
                trail.addMissingLevel();
            }

            return result.getResult();
        } else

        {
            return PredicateUtil.evaluate(predicate, context);
        }
    }

    private Trail handleTrue(Trail trail, Node node, EvaluationContext context){

        // A "true" leaf node
        if(!node.hasNodes()){
            return trail.selectNode(node);
        }

        trail.push(node);

        List<Node> children = node.getNodes();
        for(int i = 0, max = children.size(); i < max; i++){
            Node child = children.get(i);

            Boolean status = evaluateNode(trail, child, context);

            if(status == null){
                Trail destination = handleMissingValue(trail, node, child, context);

                if(destination != null){
                    return destination;
                }
            } else

            if(status.booleanValue()){
                return handleTrue(trail, child, context);
            }
        }

        // A "true" non-leaf node
        return handleNoTrueChild(trail);
    }

    private Trail handleDefaultChild(Trail trail, Node node, EvaluationContext context){

        // "The defaultChild missing value strategy requires the presence of the defaultChild attribute in every non-leaf Node"
        String defaultChild = node.getDefaultChild();
        if(defaultChild == null){
            throw new InvalidFeatureException(node);
        }

        trail.addMissingLevel();

        List<Node> children = node.getNodes();
        for(int i = 0, max = children.size(); i < max; i++){
            Node child = children.get(i);

            String id = child.getId();
            if(id != null && (id).equals(defaultChild)){
                // The predicate of the referenced Node is not evaluated
                return handleTrue(trail, child, context);
            }
        }

        // "Only Nodes which are immediate children of the respective Node can be referenced"
        throw new InvalidFeatureException(node);
    }

    private Trail handleNoTrueChild(Trail trail){
        TreeModel treeModel = getModel();

        NoTrueChildStrategyType noTrueChildStrategy = treeModel.getNoTrueChildStrategy();
        switch(noTrueChildStrategy){
            case RETURN_NULL_PREDICTION:
                return trail.selectNull();
            case RETURN_LAST_PREDICTION:
                Node lastPrediction = trail.getLastPrediction();

                // "Return the parent Node only if it specifies a score attribute"
                if(lastPrediction.hasScore()){
                    return trail.selectLastPrediction();
                }
                return trail.selectNull();
            default:
                throw new UnsupportedFeatureException(treeModel, noTrueChildStrategy);
        }
    }

    /**
     * @param parent The parent Node of the Node that evaluated to the missing value.
     * @param node The Node that evaluated to the missing value.
     */
    private Trail handleMissingValue(Trail trail, Node parent, Node node, EvaluationContext context){
        TreeModel treeModel = getModel();

        MissingValueStrategyType missingValueStrategy = treeModel.getMissingValueStrategy();
        switch(missingValueStrategy){
            case NULL_PREDICTION:
                return trail.selectNull();
            case LAST_PREDICTION:
                return trail.selectLastPrediction();
            case DEFAULT_CHILD:
                return handleDefaultChild(trail, parent, context);
            case NONE:
                return null;
            default:
                throw new UnsupportedFeatureException(treeModel, missingValueStrategy);
        }
    }

    private NodeScore createNodeScore(Node node, Object value){
        BiMap<String, Node> entityRegistry = getEntityRegistry();

        NodeScore result = new NodeScore(entityRegistry, node, value);

        return result;
    }

    private NodeScoreDistribution createNodeScoreDistribution(Node node, double missingValuePenalty){
        BiMap<String, Node> entityRegistry = getEntityRegistry();

        NodeScoreDistribution result = new NodeScoreDistribution(entityRegistry, node);

        if(!node.hasScoreDistributions()){
            return result;
        }

        List<ScoreDistribution> scoreDistributions = node.getScoreDistributions();

        double sum = 0;

        for(int i = 0, max = scoreDistributions.size(); i < max; i++){
            ScoreDistribution scoreDistribution = scoreDistributions.get(i);

            Double recordCount = scoreDistribution.getRecordCount();
            if(recordCount == null){
                throw new InvalidFeatureException(scoreDistribution);
            }

            sum += recordCount;
        } // End for

        for(int i = 0, max = scoreDistributions.size(); i < max; i++){
            ScoreDistribution scoreDistribution = scoreDistributions.get(i);

            Double probability = scoreDistribution.getProbability();
            if(probability == null){
                Double recordCount = scoreDistribution.getRecordCount();

                probability = (recordCount / sum);
            }

            result.put(scoreDistribution.getValue(), probability);

            Double confidence = scoreDistribution.getConfidence();
            if(confidence != null){
                result.putConfidence(scoreDistribution.getValue(), confidence * missingValuePenalty);
            }
        }

        return result;
    }

    static
    private class Trail {

        private Node lastPrediction = null;

        private Node result = null;

        private int missingLevels = 0;


        public Trail(){
        }

        public void push(Node node){
            setLastPrediction(node);
        }

        public Trail selectNull(){
            setResult(null);

            return this;
        }

        public Trail selectNode(Node node){
            setResult(node);

            return this;
        }

        public Trail selectLastPrediction(){
            setResult(getLastPrediction());

            return this;
        }

        public Node getResult(){
            return this.result;
        }

        private void setResult(Node result){
            this.result = result;
        }

        public Node getLastPrediction(){

            if(this.lastPrediction == null){
                throw new EvaluationException();
            }

            return this.lastPrediction;
        }

        private void setLastPrediction(Node lastPrediction){
            this.lastPrediction = lastPrediction;
        }

        public void addMissingLevel(){
            setMissingLevels(getMissingLevels() + 1);
        }

        public int getMissingLevels(){
            return this.missingLevels;
        }

        private void setMissingLevels(int missingLevels){
            this.missingLevels = missingLevels;
        }
    }

    private static final LoadingCache<TreeModel, BiMap<String, Node>> entityCache = CacheUtil.buildLoadingCache(new CacheLoader<TreeModel, BiMap<String, Node>>(){

        @Override
        public BiMap<String, Node> load(TreeModel treeModel){
            ImmutableBiMap.Builder<String, Node> builder = new ImmutableBiMap.Builder<>();

            builder = collectNodes(treeModel.getNode(), new AtomicInteger(1), builder);

            return builder.build();
        }

        private ImmutableBiMap.Builder<String, Node> collectNodes(Node node, AtomicInteger index, ImmutableBiMap.Builder<String, Node> builder){
            builder = EntityUtil.put(node, index, builder);

            if(!node.hasNodes()){
                return builder;
            }

            List<Node> children = node.getNodes();
            for(Node child : children){
                builder = collectNodes(child, index, builder);
            }

            return builder;
        }
    });
}