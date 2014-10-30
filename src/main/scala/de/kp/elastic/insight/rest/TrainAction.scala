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

import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.json.JsonXContent

import org.elasticsearch.rest.RestStatus.OK

import de.kp.elastic.insight.exception.AnalyticsException

import de.kp.elastic.insight.model._
import de.kp.elastic.insight.context.AnalyticsContext

import de.kp.elastic.insight.io.{TrainRequestBuilder,TrainResponseBuilder}

import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap


class TrainAction @Inject()(settings:Settings,client:Client,controller:RestController) extends BaseRestHandler(settings,client) {

  logger.info("Add TrainAction module")

  /* Registration of the URL part that is responsible for training predictive models */
  controller.registerHandler(RestRequest.Method.POST,"/{index}/{type}/_analytics/train/{service}", this)
  controller.registerHandler(RestRequest.Method.POST,"/{index}/_analytics/train/{service}", this)
  
  override protected def handleRequest(request:RestRequest,channel:RestChannel,client:Client) {

    try {

      logger.info("Train Request received")
  
      val params = getParams(request)
      logger.info("TrainAction: " + params)

      logger.info("Training started")
    
      /*
       * Build service request and send to remote service
       */
      val req = TrainRequestBuilder.build(params)
      val response = AnalyticsContext.send(req).mapTo[ServiceResponse]
      
      response.onSuccess {
        case result => onResponse(channel,request,result)
      }
    
      response.onFailure {
        case throwable => onError(channel,throwable)
	  }
        
    } catch {
      
      case e:Exception => onError(channel,e)
       
    }
    
  }

  private def getParams(request:RestRequest):Map[String,Any] = {

    val data = HashMap.empty[String,Any]
    
    /* Append request parameters */
    request.params().foreach(entry => {
      data += entry._1-> entry._2
    })
    
    /* Append content parameters */
    val params = XContentFactory.xContent(request.content()).createParser(request.content()).mapAndClose()
    params.foreach(entry => {
      data += entry._1-> entry._2
    })
      
    data.toMap

  }
  
  private def onResponse(channel:RestChannel,request:RestRequest,response:ServiceResponse) {
	            
    try {
	  
      val pretty = 
        if (request.param("pretty") != null && !"false".equalsIgnoreCase(request.param("pretty"))) true else false
	  
      val builder = TrainResponseBuilder.build(response,pretty)
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