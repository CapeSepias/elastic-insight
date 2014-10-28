package de.kp.elastic.insight.model
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

case class ServiceRequest(
  service:String,task:String,data:Map[String,String]
)

case class ServiceResponse(
  service:String,task:String,data:Map[String,String],status:String
)

case class Behavior(site:String,user:String,states:List[String])

case class Behaviors(items:List[Behavior])

case class ClusteredPoint(
  cluster:Int,distance:Double,point:LabeledPoint
)

case class ClusteredPoints(items:List[ClusteredPoint])

case class ClusteredSequence(
  cluster:Int,similarity:Double,sequence:NumberedSequence
)

case class ClusteredSequences(items:List[ClusteredSequence])

case class FField(field:String,value:Double)

case class FDetection(
  distance:Double,label:String,features:List[FField])

case class FDetections(items:List[FDetection])
  
case class BDetection(
  site:String,user:String,states:List[String],metric:Double,flag:String)

case class BDetections(items:List[BDetection])

case class LabeledPoint(
  label:String,features:Array[Double]
)

case class NumberedSequence(sid:Int,data:Array[Array[Int]])

case class Pattern(
  support:Int,itemsets:List[List[Int]])

case class Patterns(items:List[Pattern])

case class Purchase(site:String,user:String,timestamp:Long,amount:Float)
case class Purchases(items:List[Purchase])

case class Relation (
  items:List[Int],related:List[Int],support:Int,confidence:Double,weight:Double)

case class Relations(site:String,user:String,items:List[Relation])

case class MultiRelations(items:List[Relations])

case class Rule (
  antecedent:List[Int],consequent:List[Int],support:Int,confidence:Double)

case class Rules(items:List[Rule])

object ResponseStatus {
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}

object Serializer {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def deserializeBehavior(behaviors:String):Behaviors = read[Behaviors](behaviors)

  /*
   * Clustered points specify the result of the similarity analysis
   * with respect to features, and clustered sequences with respect
   * to sequenecs
   */
  def deserializeClusteredPoints(points:String):ClusteredPoints = read[ClusteredPoints](points)
  def deserializeClusteredSequences(sequences:String):ClusteredSequences = read[ClusteredSequences](sequences)

  def deserializeBDetections(detections:String):BDetections = read[BDetections](detections)
  def deserializeFDetections(detections:String):FDetections = read[FDetections](detections)
 
  def deserializeRules(rules:String):Rules = read[Rules](rules)
  def deserializeMultiRelations(relations:String):MultiRelations = read[MultiRelations](relations)
   
  def deserializePatterns(patterns:String):Patterns = read[Patterns](patterns)
  def deserializePurchases(purchases:String):Purchases = read[Purchases](purchases)
 
  def serializeRequest(request:ServiceRequest):String = write(request)
  
}

object Services {
  	
	/** 
	The Association Analysis Service discovers hidden relations in large-scale databases; 
	the respective result may be used by find hidden aspects about customer and products 
	and may help e.g. marketers to improve targeting. 
	*/
	val ASSOCIATION:String = "association"
	/** 
	The Context-Aware Analysis Service leverages context-sensitive information to e.g. provide
	personalized recommendations.
	*/
	val CONTEXT:String = "context"	
	/** 
	The Decision Analysis Service predicts the best decisions among multiple courses of action 
	and identifies their decisive factors. 
	*/
	val DECISION:String = "decision"
	/** 
	The Intent Recognition Service uncover the intents of human behavior and delivers the ultimate 
	customer understanding. 
	*/
	val INTENT:String = "intent"
	/**
	The Outlier Detection Service finds anomalies in large-scale datasets and human behavior for 
	advanced risk reduction.  
	*/
	val OUTLIER:String ="outlier"
    /**
    The Series Service detects frequent patterns and rules in activity sequences; the respective
    results may be used to predict pre- and post-behavior with respect to a specific event.
     */
	val SERIES:String = "series"
    /** 
    The Similarity Service finds relevant similarities in dynamic activity sequences and identifies 
    customers by their journeys.
    */
	val SIMILARITY:String = "similarity"
    /**
    The Social Analysis Service determines and leverages actual trends from social media platforms 
    in real-time.
    */
	val SOCIAL:String = "social"
    /**
    The Text Analysis Service language-agnostic semantic concept detection and prediction for 
    semantic targeting.
    */
	val TEXT:String = "text"
    /**
    The MetaService collects XML based metadata description that are used by the respective engines 
    to access supported data sources; e.g. for the decision service a metadata description specifies 
    which fields have to be taken into account and which of them are categorical or numerical fields.
    */
	val META:String = "meta"

	private val services = List(
	    ASSOCIATION,CONTEXT,DECISION,INTENT,OUTLIER,SERIES,SIMILARITY,SOCIAL,TEXT
	)
	
	def isService(service:String):Boolean = services.contains(service)
	
}

object Concepts {
  /**
   * Behavior is a concept used by outlier detection to specify outliers
   * in human behavior
   */
  val BEHAVIOR:String = "behavior"
  /**
   * Concepts is a concept used by text analysis to retrieve the discovered
   * topics
   */
  val CONCEPTS:String = "concepts"
  /**
   * Features is a concepts used by similarity analysis to retrieve similar
   * datasets
   */
  val FEATURES:String = "features"
  /**
   * Followers is a concept used by association analysis to retrieve rules
   * that either match the provided antecedent or consequent part; it is also
   * used by series analysis
   */
  val FOLLOWERS:String = "followers"
  /**
   * Items is a concept used by association analysis to retrieve those rules
   * that match with the latest transactions as antecedents
   */
  val ITEMS:String = "items"
  /**
   * Loyalty is used by intent recognition to predict the customers' loyalty
   * states
   */
  val LOYALTY:String = "loyalty"
  /**
   * Outliers is a concept used by outlier detection to retrieve those data
   * records that are far away from all others
   */
  val OUTLIERS:String = "outliers"
  /**
   * Patterns is used by series analysis to retrieve frequent patterns that
   * have been discovered in activity sequences
   */
  val PATTERNS:String = "patterns"
  /**
   * Prediction is a concept used by decision analysis to retrieve a target
   * value for a provided feature set; it is also used by context-aware analysis
   */  
  val PREDICTION:String = "prediction"
  /**
   * Purchase is a concept used by intent recognition to retrieve the next
   * purchase horizon
   */
  val PURCHASE:String = "purchase"
  /**
   * Rules is a concept to retrieve discovered rules from association analysis;
   * it is also used by series analysis
   */
  val RULES:String = "rules"
  /**
   * Sequences is a concept used by similarity analysis to retrieve clustered
   * behavioural sequences
   */
  val SEQUENCES:String = "sequences"
    
  private val concepts = List(
      CONCEPTS,FEATURES,FOLLOWERS,ITEMS,OUTLIERS,PATTERNS,PREDICTION,RULES,SEQUENCES      
  )
  
  def isConcept(concept:String):Boolean = concepts.contains(concept)
  
}
