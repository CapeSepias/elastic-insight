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

import de.kp.elastic.insight.model._
import de.kp.elastic.insight.exception.AnalyticsException

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

object TrackRequestBuilder {

  def build(params:Map[String,Any]):ServiceRequest = {
        
    val service = params("service").asInstanceOf[String]
    if (Services.isService(service) == false) {
      throw new AnalyticsException("No <service> found.")
      
    }
    
    val topic = params("topic").asInstanceOf[String]
    if (Elements.isElement(topic) == false) {
      throw new AnalyticsException("No indexing topic found.")
      
    }
    
    /* Build request data */
    val data = HashMap.empty[String,String]    
    data += "uid" -> params("uid").asInstanceOf[String]

    service match {

	  case "association" => {
	    
	    if (topic != Elements.ITEM) throw new AnalyticsException("Tracking topic is not valid for the service provided.")
	    
	    appendItem(params,data)
	    new ServiceRequest(service,"index",data.toMap)  
	  
	  }
	  
	  case "context" => {
	    
	    if (topic != Elements.FEATURE) throw new AnalyticsException("Tracking topic is not valid for the service provided.")
	    
	    appendFeature(params,data)
	    new ServiceRequest(service,"index",data.toMap)  

	  }
      case "decision" => {
	    
	    if (topic != Elements.FEATURE) throw new AnalyticsException("Tracking topic is not valid for the service provided.")
	    
	    appendFeature(params,data)
	    new ServiceRequest(service,"index",data.toMap)  

      }
      case "intent" => {
	    
	    topic match {	      

	      case Elements.AMOUNT => {
	    
	        appendAmount(params,data)
	        new ServiceRequest(service,"index:amount",data.toMap)  
	        
	      }
	      
	      case _ => throw new AnalyticsException("Tracking topic is not valid for the service provided.")
	      
	    }
      
      }
	  case "outlier" => {
	    
	    topic match {
	      
	      case Elements.FEATURE => {
	    
	        appendFeature(params,data)
	        new ServiceRequest(service,"index:feature",data.toMap)  
	        
	        
	      }
	      case Elements.SEQUENCE => {
	    
	        appendExtendedItem(params,data)
	        new ServiceRequest(service,"index:sequence",data.toMap)  
	        
	      }
	      
	      case _ => throw new AnalyticsException("Tracking topic is not valid for the service provided.")
	    
	    }
	    
	  }
	  case "series" => {
	    
	    if (topic != Elements.ITEM) throw new AnalyticsException("Tracking topic is not valid for the service provided.")
	    
	    appendItem(params,data)
	    new ServiceRequest(service,"index",data.toMap)  
	  
	  }

	  case "similarity" => {
	    
	    topic match {

	      case Elements.FEATURE => {
	    
	        appendFeature(params,data)
	        new ServiceRequest(service,"index:feature",data.toMap)  
	        
	      }	
	      case Elements.SEQUENCE => {
	    
	        appendItem(params,data)
	        new ServiceRequest(service,"index:sequence",data.toMap)  
	        
	      }
	      
	      case _ => throw new AnalyticsException("Tracking topic is not valid for the service provided.")
	      
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
  private def appendAmount(params:Map[String,Any],data:HashMap[String,String]) {
 
    try {
         
      data += "site" -> params("site").asInstanceOf[String]
      data += "timestamp" -> params("timestamp").asInstanceOf[String]

      data += "user" -> params("user").asInstanceOf[String]
      data += "amount" -> params("amount").asInstanceOf[String]
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid topic description for the provided service.")
      }
    
    }

  }

  /** Item is used by Association Analysis **/
  private def appendItem(params:Map[String,Any],data:HashMap[String,String]) {
 
    try {
         
      data += "site" -> params("site").asInstanceOf[String]
      data += "timestamp" -> params("timestamp").asInstanceOf[String]

      data += "user" -> params("user").asInstanceOf[String]
      data += "group" -> params("group").asInstanceOf[String]

      data += "item" -> params("item").asInstanceOf[String]
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid topic description for the provided service.")
      }
    
    }

  }
  
  private def appendExtendedItem(params:Map[String,Any],data:HashMap[String,String]) {
 
    try {
         
      data += "site" -> params("site").asInstanceOf[String]
      data += "timestamp" -> params("timestamp").asInstanceOf[String]

      data += "user" -> params("user").asInstanceOf[String]
      data += "group" -> params("group").asInstanceOf[String]

      data += "item" -> params("item").asInstanceOf[String]
      data += "price" -> params("price").asInstanceOf[String]
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid topic description for the provided service.")
      }
    
    }

  }
  
  private def appendFeature(params:Map[String,Any],data:HashMap[String,String]) {
 
    try {
    
      data += "site" -> params("site").asInstanceOf[String]
      data += "timestamp" -> params("timestamp").asInstanceOf[String]
 
      /* 
       * Restrict parameters to those that are relevant to feature description;
       * note, that we use a flat JSON data structure for simplicity and distinguish
       * field semantics by different prefixes 
       */
      val records = params.filter(kv => kv._1.startsWith("lbl.") || kv._1.startsWith("fea."))
      for (rec <- records) {
      
        val (k,v) = rec
        
        val name = k.replace("lbl.","").replace("fea.","")
        data += k -> v.asInstanceOf[String]      
      
    }
     
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid topic description for the provided service.")
      }
    
    }
    
  }
 
}