package de.kp.elastic.insight.river.handler
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

import java.io.IOException

import org.elasticsearch.client.Client
import org.elasticsearch.common.xcontent.XContentFactory

import org.elasticsearch.river.RiverSettings

import de.kp.elastic.insight.model._
import de.kp.elastic.insight.context.AnalyticsContext

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._

class LearnHandler(riverSettings:RiverSettings,client:Client) extends ActionHandler(riverSettings,client) {
    
  override def execute(service:String) {
    /*
     * Convert settings into request data thereby
     * filtering those settings that have Strings
     * as values
     * 
     */
    val data = rootSettings.filter(kv => kv._2.isInstanceOf[String]).map(kv => {
      
      val (k,v) = kv
      (k,v.asInstanceOf[String])
      
    }).toMap
    
    /*
     * Build service request and send to remote service
     */
    val req = new ServiceRequest(service,"train",data)
    val response = AnalyticsContext.send(req).mapTo[ServiceResponse]
      
    response.onSuccess {
        case result => 
          logger.info("Learning from \n " + Serializer.serializeRequest(req))
    }
    
    response.onFailure {
        case throwable => {
          logger.error("Learning failed due to: " + throwable.getMessage())
        }	 	      
	}
    
  }
  
}