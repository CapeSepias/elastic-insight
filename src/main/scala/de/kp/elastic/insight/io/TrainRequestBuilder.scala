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

class TrainRequestBuilder extends RequestBuilder {

  def build(params:Map[String,Any]):ServiceRequest = {
        
    val service = params("service").asInstanceOf[String]
    if (Services.isService(service) == false) {
      throw new AnalyticsException("No <service> found.")
      
    }
    
    val task = "train"
     
    val data = HashMap.empty[String,String]   
    try {
    
      data += Names.REQ_UID -> params(Names.REQ_UID).asInstanceOf[String]

      data += Names.REQ_SITE -> params(Names.REQ_SITE).asInstanceOf[String]
      data += Names.REQ_NAME -> params(Names.REQ_NAME).asInstanceOf[String]
 
      data += Names.REQ_SOURCE -> params(Names.REQ_SOURCE).asInstanceOf[String]
      data += Names.REQ_ALGORITHM -> params(Names.REQ_ALGORITHM).asInstanceOf[String]
    
    } catch {
      case e:Exception => throw new AnalyticsException("Not enough parameters to train dataset.") 
    }

    val supported = List(Names.REQ_ALGORITHM,Names.REQ_NAME,Names.REQ_SITE,Names.REQ_SOURCE,Names.REQ_UID)
    /*
     * Actually the validation of all other parameters is performed
     * by the different predictive engines individually
     */
    params.foreach(entry => {
      
      /* We make sure that only String parameters are sent to the
       * different predictive engines
       */
      if (entry._2.isInstanceOf[String]) {
        
        if (supported.contains(entry._1) == false) {
          data += entry._1 -> entry._2.asInstanceOf[String]
        } 
        
      }
      
    })
    
    new ServiceRequest(service,task,data.toMap)
    
  }
  
}