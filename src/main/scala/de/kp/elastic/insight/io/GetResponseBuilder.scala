package de.kp.elastic.insight.io
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Elastic-Insight project
* (https://github.com/skrusche63/elastic-insight).
* 
* Elastic-Insight is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Elastic-Insight is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Elastic-Insight. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentBuilder

import de.kp.elastic.insight.model._

class GetResponseBuilder extends ResponseBuilder {

  override def build(res:ServiceResponse,pretty:Boolean):XContentBuilder = {
      
    val builder = XContentFactory.jsonBuilder()
	if (pretty) builder.prettyPrint().lfAtEnd()

	builder
      .startObject()
        .field("service",res.service)
        .field("task",res.task)
        .field("status",res.status)
        .field("uid",res.data("uid"))

    res.service match {
      /*
       * Association Analysis
       */	  
	  case Services.ASSOCIATION => {
	      
	    if (res.data.contains(Concepts.TRANSACTION)) {
	      appendItems(builder,res.data(Concepts.TRANSACTION))
	      
	    } else if (res.data.contains(Concepts.RULE)) {
	      appendRules(builder,res.data(Concepts.RULE))
	    }
	    
	  }
	  /*
	   * Context-Aware Analysis
	   */
	  case Services.CONTEXT => {
	    
	    if (res.data.contains(Concepts.PREDICTION)) {
	      appendPrediction(builder,res.data(Concepts.PREDICTION))
	      
	    }
	    
	  }
	  /*
	   * Decision Analysis
	   */
	  case Services.DECISION => {
	    
	    if (res.data.contains(Concepts.PREDICTION)) {
	      appendPrediction(builder,res.data(Concepts.PREDICTION))
	      
	    }
	    
	  }
	  /*
	   * Intent Recognition
	   */
	  case Services.INTENT => {
	    
	    if (res.data.contains(Concepts.LOYALTY)) {
	      appendLoyalty(builder,res.data(Concepts.LOYALTY))
	      
	    } else if (res.data.contains(Concepts.PURCHASE)) {
	      appendPurchase(builder,res.data(Concepts.PURCHASE))
	      
	    }
	    
	  }
	  /*
	   * Outlier Detection
	   */
	  case Services.OUTLIER => {
	    
	    if (res.data.contains(Concepts.BEHAVIOR)) {
	      appendBDetection(builder,res.data(Concepts.BEHAVIOR))
	    
	    } else if (res.data.contains(Concepts.FEATURE)) {
	      appendFDetection(builder,res.data(Concepts.FEATURE))
	      
	    } 

	  }
	  /*
	   * Series Analysis
	   */
	  case Services.SERIES => {
	      
	    if (res.data.contains(Concepts.PATTERN)) {
	      appendPatterns(builder,res.data(Concepts.PATTERN))
	      
	    } else if (res.data.contains(Concepts.RULE)) {
	      appendRules(builder,res.data(Concepts.RULE))
	    }
	    
	  }
      /*
       * Similarity Analysis
       */	  
	  case Services.SIMILARITY => {
	    
	    if (res.data.contains(Concepts.FEATURE)) {
	      appendFeatures(builder,res.data(Concepts.FEATURE))
	      
	    } else if (res.data.contains(Concepts.SEQUENCE)) {
	      appendSequences(builder,res.data(Concepts.SEQUENCE))
	      
	    }
	    
	  }
	  /*
	   * Social Analysis
	   */
	  case Services.SOCIAL => {
	    /* not implemented yet */
	  }
	  /*
	   * Text Analysis
	   */
	  case Services.TEXT => {
	    /* not implemented yet */
	  }
	}
          
	builder.endObject()
    builder
    
  }
  private def appendBDetection(builder:XContentBuilder,detection:String) = {
     
    val data = Serializer.deserializeBDetections(detection)

    builder.startObject("result")
	builder.field("total", data.items.size)
	  
	builder.startArray("data")    
    for (record <- data.items) {
      
      builder.startObject()
       /* site */
      builder.field("site",record.site)
      /* user */
      builder.field("user",record.user)
      /* metric */
      builder.field("metric",record.metric)
      /* flag */
      builder.field("flag",record.flag)
      /* states */
      builder.startArray("states")
      record.states.foreach(v => builder.value(v))
      builder.endArray()
      
      builder.endObject()
      
    }
	  
    builder.endArray()	  
	builder.endObject()
   
  }
  
  private def appendFDetection(builder:XContentBuilder,detection:String) = {
     
    val data = Serializer.deserializeFDetections(detection)

    builder.startObject("result")
	builder.field("total", data.items.size)
	  
	builder.startArray("data")    
    for (record <- data.items) {
      
      builder.startObject()
      /* distance */
      builder.field("distance",record.distance)
      /* label */
      builder.field("label",record.label)
      /* features */
      builder.startArray("features")
      for (feature <- record.features) {
        builder.startObject()
        
        builder.field("field",feature.field)
        builder.field("value",feature.value)
        
        builder.endObject()
      }
      
      builder.endArray()
      
      builder.endObject()
      
    }
	  
    builder.endArray()	  
	builder.endObject()
    
  }
  
  private def appendFeatures(builder:XContentBuilder,features:String) = {
     
    val data = Serializer.deserializeClusteredPoints(features)

    builder.startObject("result")
	builder.field("total", data.items.size)
	  
	builder.startArray("data")    
    for (record <- data.items) {
      
      builder.startObject()
      
      /* cluster */
      builder.field("cluster",record.cluster)
      /* distance */
      builder.field("distance",record.distance)
      
      /* point */      
      builder.startObject("point")
      
      /* label */
      builder.field("label",record.point.label)
      /* features */
      builder.startArray("features")
      record.point.features.foreach(v => builder.value(v))
      builder.endArray()
      
      builder.endObject()      
      builder.endObject()
      
    }
	  
    builder.endArray()	  
	builder.endObject()
    
  }
  
  private def appendItems(builder:XContentBuilder,relations:String) {
     
    val data = Serializer.deserializeMultiRelations(relations)

    builder.startObject("result")
	builder.field("total", data.items.size)
	  
	builder.startArray("data")    
	for (weightedRules <- data.items) {

	  builder.startObject()
	  
	  builder.field("site",weightedRules.site)
	  builder.field("user",weightedRules.user)
	  
	  builder.startArray("rules")
	  for (weightedRule <- weightedRules.items) {
	    
	    builder.startObject()

	    /* items */
	    builder.startArray("antecedent")
	    weightedRule.antecedent.foreach(v => builder.value(v))
	    builder.endArray()

	    /* related */
	    builder.startArray("consequent")
	    weightedRule.consequent.foreach(v => builder.value(v))
	    builder.endArray()
	    
	    /* relation parameters */
	    builder.field("support",weightedRule.support)
	    builder.field("confidence",weightedRule.confidence)
	    
	    builder.field("weight",weightedRule.weight)

	    builder.endObject()
	    
	  }
	
	}
	  
    builder.endArray()	  
	builder.endObject()

  }
  
  private def appendLoyalty(builder:XContentBuilder,loyalty:String) {
    
    val data = Serializer.deserializeBehavior(loyalty)

    builder.startObject("result")
	builder.field("total", data.items.size)
	  
	builder.startArray("data")    
    for (record <- data.items) {
      
      builder.startObject()
      
	  /* site */
      builder.field("site",record.site)
      /* user */
      builder.field("user",record.user)
      /* states */
      builder.startArray("states")
      record.states.foreach(v => builder.value(v))
      builder.endArray()
      
      builder.endObject
	
	}
	  
    builder.endArray()	  
	builder.endObject()
   
  }
  
  private def appendPatterns(builder:XContentBuilder,patterns:String) {
   
    val data = Serializer.deserializePatterns(patterns)

    builder.startObject("result")
	builder.field("total", data.items.size)
	  
	builder.startArray("data")    
    for (record <- data.items) {
      
      builder.startObject()
	  /* support */
      builder.field("support",record.support)
      /* level */
      builder.field("level",record.itemsets.length)
      
      /* itemsets */
      builder.startArray("itemsets")
      for (itemset <- record.itemsets) {
        builder.startArray()
        itemset.foreach(v => builder.value(v))
        builder.endArray()
      }
      
      builder.endArray()      
	  builder.endObject()
	
	}
	          
    builder.endArray()
	builder.endObject()
    
  }
  
  private def appendPurchase(builder:XContentBuilder,purchases:String) {
    
    val data = Serializer.deserializePurchases(purchases)
    
    builder.startObject("result")
	builder.field("total", data.items.size)
	
	builder.startArray("data")	  
	for (record <- data.items) {
	  
	  builder.startObject()
	  /* site */
      builder.field("site",record.site)
      /* user */
      builder.field("user",record.user)
	  /* timestamp */
      builder.field("timestamp",record.timestamp)
      /* amount */
      builder.field("amount",record.amount)
	  
	  builder.endObject()
	
	}
	          
    builder.endArray()
	builder.endObject()
    
  }
  
  private def appendRules(builder:XContentBuilder,rules:String) {
     
    val data = Serializer.deserializeRules(rules)
    
    builder.startObject("result")
	builder.field("total", data.items.size)
	
	builder.startArray("data")	  
	for (rule <- data.items) {
	  
	  builder.startObject()
	  
	  /* antecendent */
	  builder.startArray("antecedent")
	  rule.antecedent.foreach(v => builder.value(v))
	  builder.endArray()
	  
	  /* consequent */
	  builder.startArray("consequent")
	  rule.consequent.foreach(v => builder.value(v))
	  builder.endArray()
	    
	  /* rule parameters */
	  builder.field("support",rule.support)
	  builder.field("confidence",rule.confidence)
	  
	  builder.endObject()
	
	}
	          
    builder.endArray()
	builder.endObject()
    
  }
  
  private def appendPrediction(builder:XContentBuilder,prediction:String) = {

    builder.startObject("result")
    /* prediction */
    builder.field("prediction",prediction)

    builder.endObject()
    
  }
  
  private def appendSequences(builder:XContentBuilder,sequences:String) = {
   
    val data = Serializer.deserializeClusteredSequences(sequences)

    builder.startObject("result")
	builder.field("total", data.items.size)
	  
	builder.startArray("data")    
    for (record <- data.items) {
      
      builder.startObject()
      
      /* cluster */
      builder.field("cluster",record.cluster)
      /* similarity */
      builder.field("similarity",record.similarity)
      
      /* sequence */      
      val sequence = record.sequence
      builder.startObject("sequence")
      
      /* id */
      builder.field("id",sequence.sid)
      /* data */
      builder.startArray("data")
      for (itemset <- sequence.data) {
        builder.startArray()
        itemset.foreach(v => builder.value(v))
        builder.endArray()
      }
      builder.endArray()
      
      builder.endObject()      
      builder.endObject()
      
    }
	  
    builder.endArray()	  
	builder.endObject()

  }
  
}