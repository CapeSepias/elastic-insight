package de.kp.elastic.insight.rest
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

import org.elasticsearch.rest._
import org.elasticsearch.client.Client

import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory

import org.elasticsearch.rest.RestStatus.OK

import de.kp.elastic.insight.model._
import de.kp.elastic.insight.context.AnalyticsContext

import de.kp.elastic.insight.exception.AnalyticsException
import de.kp.elastic.insight.utils.StringUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._

class GetAction @Inject()(settings:Settings,client:Client,controller:RestController) extends BaseRestHandler(settings, client) {
  
  controller.registerHandler(RestRequest.Method.GET,"/{index}/{type}/_analytics/{service}/{uid}/{concept}", this)
  controller.registerHandler(RestRequest.Method.GET,"/{index}/_analytics/get/{service}/{uid}/{concept}", this)

  override protected def handleRequest(request:RestRequest,channel:RestChannel,client:Client) {
    
    val index = request.param("index")
    val mapping = request.param("type")

    val service = request.param("service")
    if (StringUtils.isBlank(service)) {

      onError(channel, new AnalyticsException("No <service> found."))
      return
    
    }

    val uid = request.param("uid")
    if (StringUtils.isBlank(uid)) {

      onError(channel, new AnalyticsException("No <uid> found."))
      return
    
    }

    val concept = request.param("concept")
    if (StringUtils.isBlank(concept)) {

      onError(channel, new AnalyticsException("No <concept> found."))
      return
    
    }
    val req = new ServiceRequest(service,"get:"+concept,Map("uid" -> uid))
    val response = AnalyticsContext.send(req).mapTo[ServiceResponse]
      
    response.onSuccess {
        case result => onResponse(channel,request,result)
    }
    
    response.onFailure {
        case throwable => onError(channel,throwable)
	}
    
  }

  private def onResponse(channel:RestChannel,request:RestRequest,response:ServiceResponse) {
	            
    try {
	  
      val builder = XContentFactory.jsonBuilder()
	  
      val pretty = request.param("pretty")
	  if (pretty != null && !"false".equalsIgnoreCase(pretty)) {
	    builder.prettyPrint().lfAtEnd()
	  }
	  
      builder
        .startObject()
          .field("service",response.service)
          .field("task",response.status)
          .field("uid",response.data("uid"))
          .field("status",response.status)
          
          val concept = request.param("concept")
          concept match {
        
            case "associated" => {
              
              val rules = Serializer.deserializeRules(response.data("rules"))
 	          builder.startObject("rules")
	                   .field("total", rules.items.size)
	                   .startArray("items")
	          for (item <- rules.items) {
	            builder.startObject()
	                     .field("antecedent",item.antecedent.mkString("[",",","]"))
	                     .field("consequent",item.consequent.mkString("[",",","]"))
	                     .field("support",item.support)
	                     .field("confidence",item.confidence)
	                   .endObject()
	          }
	          
              builder.endArray()
	               .endObject()
              
            }
            
            case "pattern" => {
              
              val patterns = Serializer.deserializePatterns(response.data("patterns"))
 	          builder.startObject("rules")
	                   .field("total", patterns.items.size)
	                   .startArray("items")

	          for (item <- patterns.items) {
	            
	            val level = item.itemsets.size
	            val support = item.support

	            val itemsets = item.itemsets.map(_.mkString("[",",","]")).mkString("[",",","]")
	            builder.startObject()
	                     .field("level",level)
	                     .field("support",support)
	                     .field("itemsets",itemsets)
	                   .endObject()
	            
	          }
	          
              builder.endArray()
	               .endObject()
 
            }
            
            case "relation" => {
              
              val relations = response.data("relations")
              // TODO
              
            }
              
            case "rule" => {
              
              val rules = Serializer.deserializeRules(response.data("rules"))
 	          builder.startObject("rules")
	                   .field("total", rules.items.size)
	                   .startArray("hits")
	          for (item <- rules.items) {
	            builder.startObject()
	                     .field("antecedent",item.antecedent.mkString("[",",","]"))
	                     .field("consequent",item.consequent.mkString("[",",","]"))
	                     .field("support",item.support)
	                     .field("confidence",item.confidence)
	                   .endObject()
	          }
	          
              builder.endArray()
	               .endObject()
             
            } 
              
            case _ => {}
            
          } 
          
          
	  builder.endObject()

	  channel.sendResponse(new BytesRestResponse(RestStatus.OK,builder))
	            
    } catch {
      case e:IOException => throw new AnalyticsException("Failed to build a response.", e)
    }   
    
  }
 
  private def onError(channel:RestChannel,t:Throwable) {
        
    try {
      channel.sendResponse(new BytesRestResponse(channel, t))
        
    } catch {
      case e:Throwable => logger.error("Failed to send a failure response.", e);
  
    }
    
  }

}