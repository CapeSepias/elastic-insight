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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.elastic.insight.model._
import de.kp.elastic.insight.exception.AnalyticsException

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

class GetRequestBuilder extends RequestBuilder {

  override def build(params:Map[String,Any]):ServiceRequest = {
        
    val service = params("service").asInstanceOf[String]
    if (Services.isService(service) == false) {
      throw new AnalyticsException("No <service> found.")
      
    }

    val subject = params("subject").asInstanceOf[String]
    
    val data = HashMap.empty[String,String]    
    data += Names.REQ_UID -> params(Names.REQ_UID).asInstanceOf[String]

    data += Names.REQ_SITE -> params(Names.REQ_SITE).asInstanceOf[String]
    data += Names.REQ_NAME -> params(Names.REQ_NAME).asInstanceOf[String]
   
    service match {
      
      case Services.ASSOCIATION => {

        val topics = List("antecedent","consequent","crule","rule")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")

        if (List("antecedent","consequent").contains(subject)) {

          val items = params("items").asInstanceOf[List[Int]]
          data += Names.REQ_ITEMS -> items.mkString(",")
          
        }
        
        val task = "get:" + subject
        new ServiceRequest(service, task, data.toMap)

      }
      case Services.CONTEXT => {
        /*
         * Context Analysis does not support 'get' requests directly; therefore
         * these requests are mapped to 'predict' or 'similar'
         */
        val topics = List("prediction","similars")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")

        if (subject == "prediction") {

          val features = params("features").asInstanceOf[List[Double]]
          data += Names.REQ_FEATURES -> features.mkString(",")
          
          val task = "predict:vector"
          new ServiceRequest(service, task, data.toMap)
            
        } else {
          
          val total = params("total").asInstanceOf[Int]
          data += Names.REQ_TOTAL -> total.toString
          
          if (params.contains("columns")) {
            
            val columns = params("columns").asInstanceOf[List[Int]]
            data += Names.REQ_COLUMNS -> columns.mkString(",")
          
          } else {
          
            val start = params("start").asInstanceOf[Int]
            data += Names.REQ_START -> start.toString
          
            val end = params("end").asInstanceOf[Int]
            data += Names.REQ_END -> end.toString
             
          }
          
          val task = "similar:vector"
          new ServiceRequest(service, task, data.toMap)
          
        }
        throw new AnalyticsException("Context Analysis does not support get requests.")                        
       
      }
      case Services.DECISION => {

        val topics = List("prediction")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
        
        val features = params("features").asInstanceOf[List[String]]
        data += Names.REQ_FEATURES -> features.mkString(",")
        
        val task = "get:feature"
        new ServiceRequest(service, task, data.toMap)

      }
      case Services.INTENT => {

        val topics = List("state")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")

        val steps = params(Names.REQ_STEPS).asInstanceOf[Int]
        data += Names.REQ_STEPS -> steps.toString
        
        /*
         * The Markov predictor is flexible with respect to the provided
         * combination of request parameters: 
         */
        if (params.contains(Names.REQ_STATE)) {
          /*
           * A single (last) state is provided, and starting from this
           * state a set of most probable next states is computed
           */
          val state = params(Names.REQ_STATE).asInstanceOf[String]
          data += Names.REQ_STATE -> state
        
        } else if (params.contains(Names.REQ_STATES)) {
          /*
           * A list of (latest) states is provided, and for each state,
           * a set of most probable next states is computed
           */
          val states = params(Names.REQ_STATES).asInstanceOf[List[String]]
          data += Names.REQ_STATES -> states.mkString(",")
          
        }
        
        val task = "get:" + subject
        new ServiceRequest(service, task, data.toMap)
        
      }
      case Services.OUTLIER => {

        val topics = List("state","vector")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
        
        val task = "get:" + subject
        new ServiceRequest(service, task, data.toMap)

      }
      case Services.SERIES => {
    
        val topics = List("antecedent","consequent","pattern","rule")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")

        if (List("antecedent","consequent").contains(subject)) {

          val items = params("items").asInstanceOf[List[Int]]
          data += Names.REQ_ITEMS -> items.mkString(",")
          
        }
         
        val task = "get:" + subject
        new ServiceRequest(service, task, data.toMap)
       
      }
      case Services.SIMILARITY => {
    
        val topics = List("sequence","vector")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
        
        val task = "get:" + subject
        new ServiceRequest(service, task, data.toMap)

      }
      case Services.SOCIAL => {
        /* not yet implemented */
        null
      }
      case Services.TEXT => {
         /* not yet implemented */  
        null
      }
      case _ => throw new AnalyticsException("No <service> found.")

    
    }

  }

}
