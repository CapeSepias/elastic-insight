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

object RegisterRequestBuilder {

  def build(params:Map[String,Any]):ServiceRequest = {
        
    val service = params("service").asInstanceOf[String]
    if (Services.isService(service) == false) {
      throw new AnalyticsException("No <service> found.")
      
    }
    
    val metadata = params("metadata").asInstanceOf[String]
    if (Fields.isMetadata(metadata) == false) {
      throw new AnalyticsException("No <metadata> found.")
      
    }
    
    /* Build request data */
    val data = HashMap.empty[String,String]    
    data += "uid" -> params("uid").asInstanceOf[String]

    service match {

	  case "association" => {
	    
	    if (metadata != Fields.FIELDS) throw new AnalyticsException("Metadata are not valid for the service provided.")
	    
	    appendMetaItem(params,data)
	    new ServiceRequest(service,"register",data.toMap)  
	  
	  }
	  
	  case "context" => {
	    
	    if (metadata != Fields.FEATURES) throw new AnalyticsException("Metadata are not valid for the service provided.")

	    appendMetaNames(params,data)
	    new ServiceRequest(service,"register",data.toMap)  

	  }
      case "decision" => {
	    
	    if (metadata != Fields.FEATURES) throw new AnalyticsException("Metadata are not valid for the service provided.")
	    
	    appendMetaNames(params,data)
	    appendMetaTypes(params,data)
	    
	    new ServiceRequest(service,"register",data.toMap)  

      }
      case "intent" => {
	    
	    metadata match {	      

	      case Fields.LOYALTY => {
	    
	        appendMetaAmount(params,data)
	        new ServiceRequest(service,"register:loyalty",data.toMap)  
	        
	      }
	      case Fields.PURCHASE => {
	    
	        appendMetaAmount(params,data)
	        new ServiceRequest(service,"register:purchase",data.toMap)  
	        
	      }
	      
	      case _ => throw new AnalyticsException("Metadata are not valid for the service provided.")
	      
	    }
      
      }
	  case "outlier" => {
	    
	    metadata match {
	      
	      case Fields.FEATURES => {
	    
	        appendMetaNames(params,data)
	        appendMetaTypes(params,data)
	    
	        new ServiceRequest(service,"register:features",data.toMap)  
	        
	        
	      }
	      case Fields.SEQUENCES => {
	    
	        appendMetaExtendedItem(params,data)
	        new ServiceRequest(service,"register:sequences",data.toMap)  
	        
	      }
	      
	      case _ => throw new AnalyticsException("Metadata are not valid for the service provided.")
	    
	    }
	    
	  }
	  case "series" => {
	    
	    if (metadata != "fields") throw new AnalyticsException("Metadata are not valid for the service provided.")
	    
	    appendMetaItem(params,data)
	    new ServiceRequest(service,"register",data.toMap)  
	  
	  }

	  case "similarity" => {
	    
	    metadata match {

	      case Fields.FEATURES => {
	    
	        appendMetaNames(params,data)
	        appendMetaTypes(params,data)
	    
	        new ServiceRequest(service,"register:features",data.toMap)  
	        
	      }	
	      case Fields.SEQUENCES => {
	    
	        appendMetaItem(params,data)
	        new ServiceRequest(service,"register:sequences",data.toMap)  
	        
	      }
	      
	      case _ => throw new AnalyticsException("Metadata are not valid for the service provided.")
	      
	    }
	    
	  }
	  case "social" => {
	    /* Not implemented yet */
	  }
	  case "text" => {
	    /* Not implemented yet */
	  }

	  case _ => throw new AnalyticsException("Unknown service.")
	  
    }
    
    null
    
  }
  
  private def appendMetaAmount(params:Map[String,Any],data:HashMap[String,String]) {
 
    try {
         
      data += "site" -> params("site").asInstanceOf[String]
      data += "timestamp" -> params("timestamp").asInstanceOf[String]

      data += "user" -> params("user").asInstanceOf[String]
      data += "amount" -> params("amount").asInstanceOf[String]
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }

  }
	        
  private def appendMetaExtendedItem(params:Map[String,Any],data:HashMap[String,String]) {
 
    try {
         
      data += "site" -> params("site").asInstanceOf[String]
      data += "timestamp" -> params("timestamp").asInstanceOf[String]

      data += "user" -> params("user").asInstanceOf[String]
      data += "group" -> params("group").asInstanceOf[String]

      data += "item" -> params("item").asInstanceOf[String]
      data += "price" -> params("price").asInstanceOf[String]
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }

  }

  private def appendMetaItem(params:Map[String,Any],data:HashMap[String,String]) {
 
    try {
         
      data += "site" -> params("site").asInstanceOf[String]
      data += "timestamp" -> params("timestamp").asInstanceOf[String]

      data += "user" -> params("user").asInstanceOf[String]
      data += "group" -> params("group").asInstanceOf[String]

      data += "item" -> params("item").asInstanceOf[String]
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }

  }
  
  private def appendMetaNames(params:Map[String,Any],data:HashMap[String,String]) {
 
    try {
              
      val names = params("names").asInstanceOf[List[String]]
      data += "names" -> names.mkString(",")
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }

  }
 
  private def appendMetaTypes(params:Map[String,Any],data:HashMap[String,String]) {
 
    try {
              
      val types = params("types").asInstanceOf[List[String]]
      data += "names" -> types.mkString(",")
      
    } catch {
      
      case e:Exception => {
        throw new AnalyticsException("Invalid metadata description for the provided service.")
      }
    
    }

  }

}