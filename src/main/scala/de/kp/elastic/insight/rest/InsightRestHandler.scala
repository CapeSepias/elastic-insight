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
import org.elasticsearch.rest._

import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

abstract class InsightRestHandler(settings:Settings,client:Client) extends BaseRestHandler(settings, client) {

  protected def getParams(request:RestRequest):Map[String,Any] = {

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
 
  protected def onError(channel:RestChannel,t:Throwable) {
        
    try {
      channel.sendResponse(new BytesRestResponse(channel, t))
        
    } catch {
      case e:Throwable => logger.error("Failed to send a failure response.", e);
  
    }
    
  }

}