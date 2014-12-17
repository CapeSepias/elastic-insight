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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

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
	    
	val status = res.status
	if (status == ResponseStatus.SUCCESS) {
	  
	  val response = res.data(Names.REQ_RESPONSE)
      builder.startObject("result")
	  
      val Array(task,topic) = res.task.split(":")
	  res.service match {
        /*
         * Association Analysis
         */	  
	    case Services.ASSOCIATION => {
	      
	      if (List("antecedent","consequent","rule").contains(topic)) {
	        addRules(builder,response)
	        
	      } else if (task == "crule") {
	        addCRules(builder,response)
	        
	      } else {
	        // TODO
	      }
	      
	    }
	    /*
	     * Context-Aware Analysis
	     */
	    case Services.CONTEXT => {
	    
	      if (task == "predict")
            builder.field("prediction",response)
	      
	      if (task == "similar") {
	        // TODO
	      }
	    
	    }
	    /*
	     * Decision Analysis
	     */
	    case Services.DECISION => {
          builder.field("prediction",response)
	    }
	    /*
	     * Intent Recognition
	     */
	    case Services.INTENT => {
	    
	      if (topic == "loyalty")
	        addLoyalty(builder,response)
	      
	      if (topic == "purchase")
	        addPurchase(builder,response)
	    
	    }
	    /*
	     * Outlier Detection
	     */
	    case Services.OUTLIER => {
	      
	      if (topic == "featue") {
	        addFDetection(builder,response)

	      } else {
	        addBDetection(builder,response)
	        
	      }

	    }
	    /*
	     * Series Analysis
	     */
	    case Services.SERIES => {
	      
	      if (List("antecedent","consequent","rule").contains(topic)) {
	        addRules(builder,response)
	      
	      } else {
	        addPatterns(builder,response)
	      
	      }
	    
	    }
        /*
         * Similarity Analysis
         */	  
	    case Services.SIMILARITY => {
	    
	      if (topic == "feature")
	        addFeatures(builder,response)
	      
	      if (topic == "sequence")
	        addSequences(builder,response)
	      
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
	    case _ => {/* do nothing */}
	  }

	  builder.endObject()
	  
	} else {
	  /*
	   * In case of a failed request, there can be a response
	   * message to describe the respective error in more detail
	   */      
	  if (res.data.contains(Names.REQ_MESSAGE)) {
	    builder.field("message",res.data(Names.REQ_MESSAGE))
	  }
	
	}
         
	builder.endObject()
    builder
    
  }
  private def addBDetection(builder:XContentBuilder,detection:String) = {
     
    val data = Serializer.deserializeBDetections(detection)

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
   
  }
  
  private def addFDetection(builder:XContentBuilder,detection:String) = {
     
    val data = Serializer.deserializeFDetections(detection)

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
    
  }
  
  private def addFeatures(builder:XContentBuilder,features:String) = {
     
    val data = Serializer.deserializeClusteredPoints(features)

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
  
  private def addLoyalty(builder:XContentBuilder,loyalty:String) {
    
    val data = Serializer.deserializeBehavior(loyalty)

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
   
  }
  
  private def addCRules(builder:XContentBuilder,rules:String) {
     
    val data = Serializer.deserializeCRules(rules)
    
	builder.field("total", data.items.size)	
	builder.startArray("data")	  
	
	for (rule <- data.items) {
	  
	  builder.startObject()
	  
	  /* antecendent */
	  builder.startArray("antecedent")
	  rule.antecedent.foreach(v => builder.value(v))
	  builder.endArray()
	  
	  /* consequent */
	  builder.field("consequent",rule.consequent)
	    
	  /* rule parameters */
	  builder.field("support",rule.support)
	  builder.field("confidence",rule.confidence)

	  builder.field("weight",rule.weight)
	  
	  builder.endObject()
	
	}
	          
    builder.endArray()
    
  }
  
  private def addPatterns(builder:XContentBuilder,patterns:String) {
   
    val data = Serializer.deserializePatterns(patterns)

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
    
  }
  
  private def addRules(builder:XContentBuilder,rules:String) {
     
    val data = Serializer.deserializeRules(rules)
    
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
    
  }
  
  private def addPurchase(builder:XContentBuilder,purchases:String) {
    
    val data = Serializer.deserializePurchases(purchases)
    
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
    
  }
  
  private def addSequences(builder:XContentBuilder,sequences:String) = {
   
    val data = Serializer.deserializeClusteredSequences(sequences)

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

  }
  
}