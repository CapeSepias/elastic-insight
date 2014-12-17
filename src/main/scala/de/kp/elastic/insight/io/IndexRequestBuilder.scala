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

class IndexRequestBuilder extends RequestBuilder {

  def build(params:Map[String,Any]):ServiceRequest = {
        
    val service = params("service").asInstanceOf[String]
    if (Services.isService(service) == false) {
      throw new AnalyticsException("No <service> found.")
      
    }
    
    val subject = params("subject").asInstanceOf[String]
    val task = "index:" + subject
    
    val data = HashMap.empty[String,String]    
    data += Names.REQ_UID -> params(Names.REQ_UID).asInstanceOf[String]

    data += Names.REQ_SITE -> params(Names.REQ_SITE).asInstanceOf[String]
    data += Names.REQ_NAME -> params(Names.REQ_NAME).asInstanceOf[String]


    data += Names.REQ_INDEX -> params(Names.REQ_INDEX).asInstanceOf[String]
    data += Names.REQ_TYPE  -> params(Names.REQ_TYPE).asInstanceOf[String]
    
    service match {

	  case "association" => {
	    
	    val topics = List("item","rule")
	    if (topics.contains(subject)) {
	      new ServiceRequest(service,task,data.toMap) 
	      
	    } else {
	      throw new AnalyticsException("No <subject> found.")
	    }
	  
	  }
	  
	  case "context" => {
    
        val topics = List("feature")
        if (topics.contains(subject)) {
            
          val names = params(Names.REQ_NAMES).asInstanceOf[List[String]]
          data += Names.REQ_NAMES -> names.mkString(",")
      
          val types = params(Names.REQ_TYPES).asInstanceOf[List[String]]
          data += Names.REQ_TYPES -> types.mkString(",")

          new ServiceRequest(service,task,data.toMap) 
         
 	    } else {
	      throw new AnalyticsException("No <subject> found.")
	    }

	  }
      case "decision" => {
    
        val topics = List("feature")
        if (topics.contains(subject)) {
            
          val names = params(Names.REQ_NAMES).asInstanceOf[List[String]]
          data += Names.REQ_NAMES -> names.mkString(",")
      
          val types = params(Names.REQ_TYPES).asInstanceOf[List[String]]
          data += Names.REQ_TYPES -> types.mkString(",")

          new ServiceRequest(service,task,data.toMap) 
         
 	    } else {
	      throw new AnalyticsException("No <subject> found.")
	    }

      }
      case "intent" => {
	    
	    val topics = List("amount")
	    if (topics.contains(subject)) {
	      new ServiceRequest(service,task,data.toMap) 
	      
	    } else {
	      throw new AnalyticsException("No <subject> found.")
	    }
      
      }
	  case "outlier" => {
   
        val topics = List("feature","product")
        if (topics.contains(subject)) {
	    
          if (subject == "feature") {
            
            val names = params(Names.REQ_NAMES).asInstanceOf[List[String]]
            data += Names.REQ_NAMES -> names.mkString(",")
      
            val types = params(Names.REQ_TYPES).asInstanceOf[List[String]]
            data += Names.REQ_TYPES -> types.mkString(",")

          }

          new ServiceRequest(service,task,data.toMap) 
         
 	    } else {
	      throw new AnalyticsException("No <subject> found.")
	    }
	    
	  }
	  case "series" => {
	    
	    val topics = List("item","rule")
	    if (topics.contains(subject)) {
	      new ServiceRequest(service,task,data.toMap) 
	      
	    } else {
	      throw new AnalyticsException("No <subject> found.")
	    }
	    
	  }
	  case "similarity" => {
    
        val topics = List("feature","sequence")
        if (topics.contains(subject)) {
	    
          if (subject == "feature") {
            
            val names = params(Names.REQ_NAMES).asInstanceOf[List[String]]
            data += Names.REQ_NAMES -> names.mkString(",")
      
            val types = params(Names.REQ_TYPES).asInstanceOf[List[String]]
            data += Names.REQ_TYPES -> types.mkString(",")

          }

          new ServiceRequest(service,task,data.toMap) 
         
 	    } else {
	      throw new AnalyticsException("No <subject> found.")
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
 
}