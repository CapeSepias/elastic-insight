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

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

import java.util.{Date,Random}

import org.elasticsearch.client.Client

import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.index.IndexRequest.OpType

import org.elasticsearch.common.logging.Loggers

import org.elasticsearch.common.settings.Settings

import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.XContentFactory

import org.elasticsearch.indices.IndexAlreadyExistsException

import de.kp.elastic.insight.exception.AnalyticsException

import de.kp.elastic.insight.track.{ElasticBuilderFactory => EBF}
import de.kp.elastic.insight.utils.ListenerUtils

import org.elasticsearch.client.Requests

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer,HashMap}

class FeatureRequestHandler(settings:Settings,client:Client,topic:String) extends RequestHandler {

  private val DEFAULT_HEALTH_REQUEST_TIMEOUT:String = "30s"
  private val ERROR_LIST:String = "error.list"
       
  private val maxRetryCount = settings.getAsInt("analytics.rest.retry", 20)

  private val indexCreationLock = new ReentrantLock()
  private val random = new Random()
    
  private val commonPool = new ForkJoinPool()
  private val logger = Loggers.getLogger(getClass(), settings)
 
  
  override def execute(params:Params,listener:OnErrorListener,requestMap:Map[String,Any], paramMap:HashMap[String,Any],chain:RequestHandlerChain) {
 
    try {

      writeFeature(params,listener,requestMap,paramMap,OpType.INDEX,chain)
      
    } catch {
      case e:Exception => {
        
        val errorList = getErrorList(paramMap)
        if (errorList.size >= maxRetryCount) {
          listener.onError(e)
            
        } else {
          
          sleep(e)
          errorList += e
                
          fork(new Runnable() {
            override def run() {
			  execute(params, listener,requestMap,paramMap,chain)
			}
          })
            
        }
      }
    }
    
  }
    
  private def writeFeature(params:Params,listener:OnErrorListener,requestMap:Map[String,Any],paramMap:HashMap[String,Any],opType:OpType,chain:RequestHandlerChain) {
   	
    val index = params.param("index")
    val mapping = params.param("type")
    
    val responseListener = new ListenerUtils.OnIndexResponseListener[IndexResponse]() {
      override def onResponse(response:IndexResponse) {
	    logger.info("")
      }      
    }
		
	val failureListener = new ListenerUtils.OnFailureListener() {
      override def onFailure(t:Throwable) {
	            
		  val errorList = getErrorList(paramMap)
	      if (errorList.size >= maxRetryCount) {
	        listener.onError(t)
	      
	      } else {
	        
	        sleep(t)
	        errorList += t
	                    
	        doIndexExists(params,listener,requestMap,paramMap,chain)
	      
	      }
				
	  }

	}
                
    /* Update index operation */
    val feature = requestMap.get("feature") match {
      
      case None => throw new AnalyticsException("Feature is null.")
      case Some(valu) => valu.asInstanceOf[Map[String,Any]]
    
    }

    val content = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)
    content.map(EBF.getSource(topic,feature))

    client.prepareIndex(index, mapping).setSource(content).setRefresh(true).setOpType(opType)
      .execute(ListenerUtils.onIndex(responseListener, failureListener))
    
  }
  
  private def doIndexExists(params:Params,listener:OnErrorListener,requestMap:Map[String,Any], paramMap:HashMap[String,Any],chain:RequestHandlerChain) {
        
    val index = params.param("index")
    try {
      
      indexCreationLock.lock()
            
      val indicesExistsResponse = client.admin().indices().prepareExists(index).execute().actionGet()            
      if (indicesExistsResponse.isExists()) {
        
        doMappingCreation(params,listener,requestMap,paramMap,chain)
            
      } else {
        
        doIndexCreation(params,listener,requestMap,paramMap,chain, index)
            
      }
    } catch {
      case e:Exception => {
            
        val errorList = getErrorList(paramMap)
        if (errorList.size >= maxRetryCount) {
          listener.onError(e)
            
        } else {
          
          sleep(e)
          errorList += e
                
          fork(new Runnable() {
            override def run() {
			  execute(params, listener, requestMap, paramMap,chain)
			}
           })            
        }
      }
       
    } finally {
     indexCreationLock.unlock()
    }
    
  }

  private def doIndexCreation(params:Params,listener:OnErrorListener,requestMap:Map[String,Any], paramMap:HashMap[String,Any],chain:RequestHandlerChain, index:String) {
    
    try {
      
      val createIndexResponse = client.admin().indices().prepareCreate(index).execute().actionGet()            
      if (createIndexResponse.isAcknowledged()) {
        
        doMappingCreation(params,listener,requestMap,paramMap,chain)
            
      } else {
        listener.onError(new AnalyticsException("Failed to create " + index))
            
      }
    
    } catch {
      
    case e:IndexAlreadyExistsException => 	
        	
        fork(new Runnable() {
          override def run() {
			doIndexExists(params,listener,requestMap,paramMap,chain)
          }
        })
        
      case e:Exception => {
            
        val errorList = getErrorList(paramMap)
        if (errorList.size >= maxRetryCount) {
          listener.onError(e)
            
        } else {
          
          sleep(e)
          errorList += e
        
          fork(new Runnable() {
            override def run() {
    		  execute(params, listener, requestMap, paramMap,chain)
            }				
          })

        }
      }
    }
    
  }
    
  private def doMappingCreation(params:Params,listener:OnErrorListener,requestMap:Map[String,Any], paramMap:HashMap[String,Any],chain:RequestHandlerChain) {

    val index = params.param("index")
    val mapping = params.param("type")
        
    val timeout = params.param("timeout")

    try {
      
      val healthResponse = client.admin().cluster().prepareHealth(index)
                             .setWaitForYellowStatus()
                             .setTimeout(params.param(timeout))
                             .execute().actionGet()
                             
            
      if (healthResponse.isTimedOut()) {
        listener.onError(new AnalyticsException("Failed to create index: " + index + "/" + mapping))
      }

      /*
       * Define mapping schema for index 'index' and 'type'; we have
       * to extract the fields dynamically from the request data
       */
      val feature = requestMap.get("feature") match {
      
        case None => throw new AnalyticsException("Feature is null.")
        case Some(valu) => valu.asInstanceOf[Map[String,Any]]
    
      }
      
      val (names,types) = EBF.getFields(topic, feature)      
      val builder = EBF.getBuilder(topic,mapping,names,types)

      val mappingResponse = client.admin().indices().preparePutMapping(index).setType(mapping).setSource(builder).execute().actionGet()            
      if (mappingResponse.isAcknowledged()) {
            	
        fork(new Runnable() {
          
          override def run() {
			execute(params, listener, requestMap, paramMap,chain)
		  }
        })

      } else {
        listener.onError(new AnalyticsException("Failed to create mapping for " + index + "/" + mapping))
            
      }
        
    } catch {
      case e:Exception => listener.onError(e)
    }
    
  }

  private def getErrorList(paramMap:HashMap[String,Any]):ArrayBuffer[Throwable] = {

    var errorList = paramMap.get(ERROR_LIST).asInstanceOf[ArrayBuffer[Throwable]]
    if (errorList == null) {
      errorList = ArrayBuffer.empty[Throwable]
        paramMap += ERROR_LIST -> errorList
    }
        
    errorList
    
  }

  private def fork(task:Runnable) {
    commonPool.execute(task)        
  }
    
  private def sleep(t:Throwable) {
        
    val waitTime = random.nextInt(2500) + 500
    if (logger.isDebugEnabled()) {
      
      val msg = String.format("""Waiting for %s ms and retrying... The cause is: %s""",waitTime.toString,t.getMessage)
      logger.debug(msg,t)
    }
        
    try {
       Thread.sleep(waitTime)
        
    } catch {
      case e1:InterruptedException => {}
        
    }
    
  }

}