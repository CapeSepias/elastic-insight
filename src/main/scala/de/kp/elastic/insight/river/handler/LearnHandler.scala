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

class LearnHandler(riverSettings:RiverSettings,client:Client) extends ActionHandler(riverSettings,client) {

  private val ANTECEDENT_FIELD:String = "antecedent"
  private val CONSEQUENT_FIELD:String = "consequent"

  private val CONFIDENCE_FIELD:String = "confidence"
  private val SUPPORT_FIELD:String    = "support"
    
  private val RULES_FIELD:String     = "rules"
  private val TIMESTAMP_FIELD:String = "timestamp"
  private val UID_FIELD:String       = "uid"
    
  override def execute(service:String) {
    
    /*
     * Build service request and send to remote service
     */
    val req = new ServiceRequest(service,"train",Map.empty[String,String])
    val response = AnalyticsContext.send(req).mapTo[ServiceResponse]
      
    response.onSuccess {
        case result => {
       }
    }
    
    response.onFailure {
        case result => {
          
        }	 	      
	}
    
  }
}