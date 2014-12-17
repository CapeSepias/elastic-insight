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

class RegisterRequestBuilder extends RequestBuilder {

  def build(params:Map[String,Any]):ServiceRequest = {
        
    val service = params("service").asInstanceOf[String]
    if (Services.isService(service) == false) {
      throw new AnalyticsException("No <service> found.")
      
    }
    val subject = params("subject").asInstanceOf[String]
    val task = "register:" + subject
    
    val data = HashMap.empty[String,String]    
    data += Names.REQ_UID -> params(Names.REQ_UID).asInstanceOf[String]

    data += Names.REQ_SITE -> params(Names.REQ_SITE).asInstanceOf[String]
    data += Names.REQ_NAME -> params(Names.REQ_NAME).asInstanceOf[String]

    service match {

	  case "association" => {
	    
	    val topics = List("item")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    val reqdata = data ++ appendMetaItem(params)
	    new ServiceRequest(service,task,reqdata.toMap)  
	  
	  }
	  
	  case "context" => {
	    
	    val topics = List("feature")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")

        val reqdata = data ++ appendMetaNames(params) ++ appendMetaTypes(params)
	    new ServiceRequest(service,task,reqdata.toMap)  

	  }
      case "decision" => {
	    
	    val topics = List("feature")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")

        val reqdata = data ++ appendMetaNames(params) ++ appendMetaTypes(params)
	    new ServiceRequest(service,task,reqdata.toMap)  

     }
      case "intent" => {
	    
	    val topics = List("amount")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    val reqdata = data ++ appendMetaAmount(params)
	    new ServiceRequest(service,task,reqdata.toMap)  
      
      }
	  case "outlier" => {
	    
	    val topics = List("feature","product")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    if (subject == "feature") {

	      val reqdata = data ++ appendMetaNames(params) ++ appendMetaTypes(params)
	      new ServiceRequest(service,task,reqdata.toMap)  
	      
	    } else {
	    
	      val reqdata = data ++ appendMetaProduct(params)
	      new ServiceRequest(service,task,reqdata.toMap)  
	      
	    }
	    
	  }
	  case "series" => {
	    
	    val topics = List("item")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    val reqdata = data ++ appendMetaItem(params)
	    new ServiceRequest(service,task,reqdata.toMap)  
	  
	  }

	  case "similarity" => {
	    
	    val topics = List("feature","sequence")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    if (subject == "feature") {

	      val reqdata = data ++ appendMetaNames(params) ++ appendMetaTypes(params)
	      new ServiceRequest(service,task,reqdata.toMap)  
	      
	    } else {
	    
	      val reqdata = data ++ appendMetaSequence(params)
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
  
  private def appendMetaAmount(params:Map[String,Any]):HashMap[String,String] = {
 
    val data = HashMap.empty[String,String]
    try {
         
      data += Names.TIMESTAMP_FIELD -> params(Names.TIMESTAMP_FIELD).asInstanceOf[String]

      data += Names.USER_FIELD -> params(Names.USER_FIELD).asInstanceOf[String]
      data += Names.AMOUNT_FIELD -> params(Names.AMOUNT_FIELD).asInstanceOf[String]
      
      data
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }

  }

  private def appendMetaItem(params:Map[String,Any]):HashMap[String,String] = {
 
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
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }

  }
	        
  private def appendMetaProduct(params:Map[String,Any]):HashMap[String,String] = {
 
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
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }

  }

  private def appendMetaSequence(params:Map[String,Any]):HashMap[String,String] = {
 
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
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }
  
  }
  
  private def appendMetaNames(params:Map[String,Any]):HashMap[String,String] = {
 
    val data = HashMap.empty[String,String]
    try {
              
      val names = params(Names.REQ_NAMES).asInstanceOf[List[String]]
      data += Names.REQ_NAMES -> names.mkString(",")
      
      data
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }

  }
 
  private def appendMetaTypes(params:Map[String,Any]):HashMap[String,String] = {
 
    val data = HashMap.empty[String,String]
    try {
              
      val types = params(Names.REQ_TYPES).asInstanceOf[List[String]]
      data += Names.REQ_TYPES -> types.mkString(",")
      
      data
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }

  }

}