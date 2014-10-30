package de.kp.elastic.insight.context
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

import de.kp.elastic.insight.RemoteClient
import de.kp.elastic.insight.model._

import scala.concurrent.Future
import scala.collection.mutable.HashMap

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object AnalyticsContext {

 private val clientPool = HashMap.empty[String,RemoteClient]
 
  def send(req:ServiceRequest):Future[Any] = {
   
    val service = req.service
    /*
     * Determine whether the service is registered and 
     * return a failure response in case of an unknown
     * service
     */
    val response = if (Services.isService(service)) {
 
      if (clientPool.contains(service) == false) {
        clientPool += service -> new RemoteClient(service)      
      }
          
      println("AnalyticsContext: send request to remote service.")
      
      val client = clientPool(service)
      client.send(req)
       
    } else {
     
      Future {
      
        val uid = req.data("uid")
        val msg = String.format("""The services requested [%s] is unknown""",service)
      
        val data = Map("uid" -> uid, "message" -> msg)
        new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
      
      }
    
    }

    response
    
  }
 
}