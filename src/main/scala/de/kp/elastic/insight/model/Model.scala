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

import de.kp.spark.core.model._

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

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
  
case class BDetection(
  site:String,user:String,states:List[String],metric:Double,flag:String)

case class BDetections(items:List[BDetection])

case class LabeledPoint(
  id:Long,label:String,features:Array[Double]
)

case class BOutliers(items:List[(String,String,List[String],Double,String)])

case class FOutliers(items:List[(Double,LabeledPoint)])

case class NumberedSequence(sid:Int,data:Array[Array[Int]])

case class Pattern(
  support:Int,itemsets:List[List[Int]])

case class Patterns(items:List[Pattern])

case class Purchase(site:String,user:String,timestamp:Long,amount:Float)
case class Purchases(items:List[Purchase])

case class WeightedRule (
  antecedent:List[Int],consequent:List[Int],support:Int,confidence:Double,weight:Double)

case class WeightedRules(site:String,user:String,items:List[WeightedRule])

case class MultiRelations(items:List[WeightedRules])

object ResponseStatus extends BaseStatus

object Serializer extends BaseSerializer {

  /*
   * Clustered points specify the result of the similarity analysis
   * with respect to features, and clustered sequences with respect
   * to sequenecs
   */
  def deserializeClusteredPoints(points:String):ClusteredPoints = read[ClusteredPoints](points)
  def deserializeClusteredSequences(sequences:String):ClusteredSequences = read[ClusteredSequences](sequences)

  def deserializeBDetections(detections:String):BDetections = read[BDetections](detections)
  def deserializeFOutliers(outliers:String):FOutliers = read[FOutliers](outliers)
 
  def deserializeMultiRelations(relations:String):MultiRelations = read[MultiRelations](relations)
   
  def deserializePatterns(patterns:String):Patterns = read[Patterns](patterns)
  
  def serializePurchases(purchases:Purchases):String = write(purchases)
  def deserializePurchases(purchases:String):Purchases = read[Purchases](purchases)
  
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

	private val services = List(
	    ASSOCIATION,CONTEXT,DECISION,INTENT,OUTLIER,SERIES,SIMILARITY,SOCIAL,TEXT
	)
	
	def isService(service:String):Boolean = services.contains(service)
	
}

object Concepts {
  /**
   * Antecedent is a concept used by association analysis and series analysis 
   * to specify which part of the rules is subject to filtering
   */
  val ANTECEDENT:String = "antecendent"
  /**
   * Behavior is a concept used by outlier detection to specify outliers
   * in human behavior
   */
  val BEHAVIOR:String = "behavior"
  /**
   * Concept is a concept used by text analysis to retrieve the discovered
   * topics
   */
  val CONCEPT:String = "concept"
  /**
   * Consequent is a concept used by association analysis and series analysis
   * to specify which part of the rules is subject to filtering
   */
  val CONSEQUENT:String = "consequent"
  /**
   * Feature is a concepts used by similarity analysis to retrieve similar
   * datasets
   */
  val FEATURE:String = "feature"
  /**
   * Loyalty is used by intent recognition to predict the customers' loyalty
   * states
   */
  val LOYALTY:String = "loyalty"
  /**
   * Outlier is a concept used by outlier detection to retrieve those data
   * records that are far away from all others
   */
  val OUTLIER:String = "outlier"
  /**
   * Patterns is used by series analysis to retrieve frequent patterns that
   * have been discovered in activity sequences
   */
  val PATTERN:String = "pattern"
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
   * Transaction is a concept used by association analysis to retrieve those rules
   * that match with the latest transactions as antecedents
   */
  val TRANSACTION:String = "transaction"
  /**
   * Rule is a concept to retrieve discovered rules from association analysis and
   * series analysis
   */
  val RULE:String = "rule"
  /**
   * Sequence is a concept used by similarity analysis to retrieve clustered
   * behavioural sequences
   */
  val SEQUENCE:String = "sequence"
    
  private val concepts = List(
    ANTECEDENT,BEHAVIOR,CONCEPT,FEATURE,LOYALTY,OUTLIER,PATTERN,PREDICTION,PURCHASE,TRANSACTION,RULE,SEQUENCE     
  )
  
  def isConcept(concept:String):Boolean = concepts.contains(concept)
  
}
/**
 * Predictiveworks. supports multiple data sources; currently sources from HDFS
 * file system, Elasticsearch, JDBC database up to PIWIK Analytics is supported 
 */
object Sources {
  /**
   * Determines that the training data have to be taken from the HDFS file system;
   * the respective path to the data must be specified in the one of the configuration
   * files that refer to the chosen predictive engine.
   */
  val FILE:String = "FILE"
  /**
   * Determines that the training data have to be taken from an Elasticsearch 
   * search index; in combination with this data source also a field specification
   * (provided by an extra request) is required.
   */
  val ELASTIC:String = "ELASTIC" 
  /**
   * Determines that the training data have to be taken from a JDBC database; 
   * in combination with this data source also a field specification (provided 
   * by an extra request) is required.
   */    
  val JDBC:String = "JDBC"   
  /**
   * Predictiveworks. supports PIWIK Analytics and 'knows' about the fields that
   * have to be used to retrieve the training data. The configuration parameters
   * to access the underlying MySQL database must be provided for the multiple
   * predictive engines individually
   */
  val PIWIK:String = "PIWIK"   
    
  private val sources = List(FILE,ELASTIC,JDBC,PIWIK)
  
  def isSource(source:String):Boolean = sources.contains(source)
  
}

object Algorithms {

  /**
   * Factorization Machines(FM) are used by Context-Aware Analysis and 
   * Text Analysis; this algorithm is usually combined with KMEANS 
   * clustering
   */
  val FM:String = "FM"
  /**
   * Hidden Markov Models are used by Intent Recognition
   */
  val HIDDEN_MARKOV:String = "HIDDEN_MARKOV"
  /**
   * The KMeans clustering algorithm is used by Outlier Detection to 
   * discover outliers in feature sets; it is also used by Similarity
   * Analysis
   */
  val KMEANS:String = "KMEANS"
  /**
   * Latent Dirichlet Allocation (LDA) is used by Text Analysis in combination
   * with KMEANS clustering to detect semantic topics from heterogeneous textual 
   * artifacts
   */
  val LDA:String = "LDA"     
  /**
   * Markov Models are used by Intent Recognition and also by Outlier 
   * Detection is case of outliers in sequential data sets
   */
  val MARKOV:String = "MARKOV"    
  /**
   * Random Forests (RF) are used by Decision Analysis for classification 
   * and regression 
   */
  val RF:String = "RF"
  /**
   * SKMEANS is used by Similarity Analysis to cluster sequential data sets
   */
  val SKMEANS:String = "SKMEANS"
  /**
   * SPADE is used by Series Analysis for pattern discovery in large scale 
   * sequential datasets
   */    
  val SPADE:String = "SPADE"
  
  /**
   * TOPK and TOPKNR algorithm is used by Association Analysis to determine
   * the most relevant associations from large-scale datasets
   */
  val TOPK:String = "TOPK"
  val TOPKNR:String = "TOPKNR"  
  /**
   * TSR is used by Series Analysis to discover rules in large scale sequential
   * data sets
   */
  val TSR:String = "TSR"
   
  private val algorithms = List(FM,HIDDEN_MARKOV,KMEANS,LDA,MARKOV,RF,SKMEANS,SPADE,TOPK,TOPKNR,TSR)
  
  def isAlgorithm(algorithm:String):Boolean = algorithms.contains(algorithm)
  
}