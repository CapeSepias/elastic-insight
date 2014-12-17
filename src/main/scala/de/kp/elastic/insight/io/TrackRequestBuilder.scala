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

class TrackRequestBuilder extends RequestBuilder {

  def build(params:Map[String,Any]):ServiceRequest = {
        
    val service = params("service").asInstanceOf[String]
    if (Services.isService(service) == false) {
      throw new AnalyticsException("No <service> found.")
      
    }
    
    val subject = params("subject").asInstanceOf[String]
    val task = "track:" + subject
     
    val data = HashMap.empty[String,String]    
    data += Names.REQ_UID -> params(Names.REQ_UID).asInstanceOf[String]

    data += Names.REQ_SITE -> params(Names.REQ_SITE).asInstanceOf[String]
    data += Names.REQ_NAME -> params(Names.REQ_NAME).asInstanceOf[String]


    data += Names.REQ_INDEX -> params(Names.REQ_INDEX).asInstanceOf[String]
    data += Names.REQ_TYPE  -> params(Names.REQ_TYPE).asInstanceOf[String]

    service match {

	  case "association" => {
	    
	    val topics = List("item")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    val reqdata = data ++ appendItem(params)
	    new ServiceRequest(service,task,reqdata.toMap)  
	  
	  }
	  
	  case "context" => {
	    
        val topics = List("feature")
        if (topics.contains(subject)) {

          val reqdata = data ++ appendFeature(params)
          new ServiceRequest(service,task,reqdata.toMap) 
         
 	    } else {
	      throw new AnalyticsException("No <subject> found.")
	    }

	  }
      case "decision" => {
	    
        val topics = List("feature")
        if (topics.contains(subject)) {

          val reqdata = data ++ appendFeature(params)
          new ServiceRequest(service,task,reqdata.toMap) 
         
 	    } else {
	      throw new AnalyticsException("No <subject> found.")
	    }


      }
      case "intent" => {
	    
	    val topics = List("amount")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    val reqdata = data ++ appendAmount(params)
	    new ServiceRequest(service,task,reqdata.toMap)  
      
      }
	  case "outlier" => {
	    
	    val topics = List("feature","product")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    if (subject == "feature") {

	      val reqdata = data ++ appendFeature(params)
	      new ServiceRequest(service,task,reqdata.toMap)  
	      
	    } else {
	    
	      val reqdata = data ++ appendProduct(params)
	      new ServiceRequest(service,task,reqdata.toMap)  
	      
	    }
	    
	  }
	  case "series" => {
	    
	    val topics = List("item")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    val reqdata = data ++ appendItem(params)
	    new ServiceRequest(service,task,reqdata.toMap)  

	  }
	  case "similarity" => {
	    
	    val topics = List("feature","sequence")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    if (subject == "feature") {

	      val reqdata = data ++ appendFeature(params)
	      new ServiceRequest(service,task,reqdata.toMap)  
	      
	    } else {
	    
	      val reqdata = data ++ appendSequence(params)
	      new ServiceRequest(service,task,reqdata.toMap)  
	      
	    }
	    
	  }
	  case "social" => {
	    /* Not implemented yet */
	    null
	  }
	  case "text" => {
	    /* Not implemented yet */
	    null
	  }

	  case _ => throw new AnalyticsException("Unknown service.")
	  
    }
    
  }
  
  /** Amount is used by Intent Recognition */
  private def appendAmount(params:Map[String,Any]):HashMap[String,String] = {
 
    val data = HashMap.empty[String,String]
    try {
         
      data += Names.TIMESTAMP_FIELD -> params(Names.TIMESTAMP_FIELD).asInstanceOf[String]

      data += Names.USER_FIELD -> params(Names.USER_FIELD).asInstanceOf[String]
      data += Names.AMOUNT_FIELD -> params(Names.AMOUNT_FIELD).asInstanceOf[String]
      
      data
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid topic description for the provided service.")
      }
    
    }

  }

  private def appendItem(params:Map[String,Any]):HashMap[String,String] = {
 
    val data = HashMap.empty[String,String]
    try {
         
      data += Names.TIMESTAMP_FIELD -> params(Names.TIMESTAMP_FIELD).asInstanceOf[String]

      data += Names.USER_FIELD -> params(Names.USER_FIELD).asInstanceOf[String]
      data += Names.GROUP_FIELD -> params(Names.GROUP_FIELD).asInstanceOf[String]

      data += Names.ITEM_FIELD -> params(Names.ITEM_FIELD).asInstanceOf[String]
      data += Names.SCORE_FIELD -> params(Names.SCORE_FIELD).asInstanceOf[String]
      
      data
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid topic description for the provided service.")
      }
    
    }

  }
  
  private def appendProduct(params:Map[String,Any]):HashMap[String,String] = {
 
    val data = HashMap.empty[String,String]
    try {
         
      data += Names.TIMESTAMP_FIELD -> params(Names.TIMESTAMP_FIELD).asInstanceOf[String]

      data += Names.USER_FIELD -> params(Names.USER_FIELD).asInstanceOf[String]
      data += Names.GROUP_FIELD -> params(Names.GROUP_FIELD).asInstanceOf[String]

      data += Names.ITEM_FIELD -> params(Names.ITEM_FIELD).asInstanceOf[String]
      data += Names.PRICE_FIELD -> params(Names.PRICE_FIELD).asInstanceOf[String]
      
      data
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid topic description for the provided service.")
      }
    
    }

  }
  
  private def appendSequence(params:Map[String,Any]):HashMap[String,String] = {
 
    val data = HashMap.empty[String,String]
    try {
         
      data += Names.TIMESTAMP_FIELD -> params(Names.TIMESTAMP_FIELD).asInstanceOf[String]

      data += Names.USER_FIELD -> params(Names.USER_FIELD).asInstanceOf[String]
      data += Names.GROUP_FIELD -> params(Names.GROUP_FIELD).asInstanceOf[String]

      data += Names.ITEM_FIELD -> params(Names.ITEM_FIELD).asInstanceOf[String]
      
      data
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid topic description for the provided service.")
      }
    
    }

  }
  
  private def appendFeature(params:Map[String,Any]):HashMap[String,String] = {
 
    val data = HashMap.empty[String,String]
    try {
    
      data += Names.TIMESTAMP_FIELD -> params(Names.TIMESTAMP_FIELD).asInstanceOf[String]
 
      /* 
       * Restrict parameters to those that are relevant to feature description;
       * note, that we use a flat JSON data structure for simplicity and distinguish
       * field semantics by different prefixes 
       */
      val records = params.filter(kv => kv._1.startsWith("lbl.") || kv._1.startsWith("fea."))
      for (rec <- records) {
      
        val (k,v) = rec
        data += k -> v.asInstanceOf[String]      
      
      }
     
      data
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid topic description for the provided service.")
      }
    
    }
    
  }
 
}