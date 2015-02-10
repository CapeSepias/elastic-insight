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
	    
        val topics = List("point")
        if (topics.contains(subject)) {

          val reqdata = data ++ appendPoint(params)
          new ServiceRequest(service,task,reqdata.toMap) 
         
 	    } else {
	      throw new AnalyticsException("No <subject> found.")
	    }

	  }
      case "decision" => {
	    
        val topics = List("point")
        if (topics.contains(subject)) {

          val reqdata = data ++ appendPoint(params)
          new ServiceRequest(service,task,reqdata.toMap) 
         
 	    } else {
	      throw new AnalyticsException("No <subject> found.")
	    }


      }
      case "intent" => {
	    
	    val topics = List("state")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    val reqdata = data ++ appendState(params)
	    new ServiceRequest(service,task,reqdata.toMap)  
      
      }
	  case "outlier" => {
	    
	    val topics = List("state","vector")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    if (subject == "state") {

	      val reqdata = data ++ appendVector(params)
	      new ServiceRequest(service,task,reqdata.toMap)  
	      
	    } else {
	    
	      val reqdata = data ++ appendVector(params)
	      new ServiceRequest(service,task,reqdata.toMap)  
	      
	    }
	    
	  }
	  case "series" => {
	    
	    val topics = List("sequence")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    val reqdata = data ++ appendSequence(params)
	    new ServiceRequest(service,task,reqdata.toMap)  

	  }
	  case "similarity" => {
	    
	    val topics = List("sequence","vector")
	    if (topics.contains(subject) == false) throw new AnalyticsException("No <subject> found.")
	    
	    if (subject == "sequence") {
	    
	      val reqdata = data ++ appendSequence(params)
	      new ServiceRequest(service,task,reqdata.toMap)  
	      
	    } else {
	    
	      val reqdata = data ++ appendVector(params)
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
  
  /** State is used by Intent Recognition */
  private def appendState(params:Map[String,Any]):HashMap[String,String] = {
 
    val data = HashMap.empty[String,String]
    try {
         
      data += Names.TIMESTAMP_FIELD -> params(Names.TIMESTAMP_FIELD).asInstanceOf[String]

      data += Names.USER_FIELD -> params(Names.USER_FIELD).asInstanceOf[String]
      data += Names.STATE_FIELD -> params(Names.STATE_FIELD).asInstanceOf[String]
      
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

  private def appendPoint(params:Map[String,Any]):HashMap[String,String] = {
 
     val data = HashMap.empty[String,String]
     try {
         
      data += Names.ROW_FIELD -> params(Names.ROW_FIELD).asInstanceOf[String]
      data += Names.COL_FIELD -> params(Names.COL_FIELD).asInstanceOf[String]
      
      data += Names.CAT_FIELD -> params(Names.CAT_FIELD).asInstanceOf[String]
      data += Names.VAL_FIELD -> params(Names.VAL_FIELD).asInstanceOf[String]
      
      data
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }
  
  }
  
  private def appendVector(params:Map[String,Any]):HashMap[String,String] = {
 
     val data = HashMap.empty[String,String]
     try {
         
      data += Names.ROW_FIELD -> params(Names.ROW_FIELD).asInstanceOf[String]
      data += Names.COL_FIELD -> params(Names.COL_FIELD).asInstanceOf[String]
      
      data += Names.LBL_FIELD -> params(Names.LBL_FIELD).asInstanceOf[String]
      data += Names.VAL_FIELD -> params(Names.VAL_FIELD).asInstanceOf[String]
      
      data
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }
  
  }
 
}