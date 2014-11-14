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

class TrainRequestBuilder extends RequestBuilder {

  def build(params:Map[String,Any]):ServiceRequest = {
        
    val service = params("service").asInstanceOf[String]
    if (Services.isService(service) == false) {
      throw new AnalyticsException("No <service> found.")
      
    }
    
    val data = HashMap.empty[String,String]
    /*
     * Determine whether common mandatory parameters are provided with
     * the train request; every request must be designated by a unique
     * identifier. This parameter is also used as a reference for sub 
     * sequent requests.
     */
    if (params.contains("uid") == false) {
      throw new AnalyticsException("Not enough parameters to train dataset: <uid> is missing.")              
    }
    
    data += "uid" -> params("uid").asInstanceOf[String]
    
    /*
     * Determine whether the request specifies a valid data source; whether
     * this specific data source is also valid for the respective service is
     * evaluated by the associated predictive engine
     */
    if (params.contains("source") == false) {
      throw new AnalyticsException("Not enough parameters to train dataset: <source> is missing.")                    
    
    } else {
      
      val source = params("source").asInstanceOf[String]
      if (Sources.isSource(source) == false) {
        throw new AnalyticsException("Unknown <source> provided.")                            
      }
      
      data += "source" -> source
      
    }

    val supported = List("algorithm","source","uid")
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
    
    new ServiceRequest(service,"train",data.toMap)
    
  }
  
}