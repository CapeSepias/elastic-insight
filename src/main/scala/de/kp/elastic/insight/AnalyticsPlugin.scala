package de.kp.elastic.insight
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

import java.util.Collection

import org.elasticsearch.common.collect.Lists
import org.elasticsearch.common.component.LifecycleComponent
import org.elasticsearch.common.inject.Module
import org.elasticsearch.common.settings.Settings

import org.elasticsearch.plugins.AbstractPlugin
import org.elasticsearch.rest.RestModule
import org.elasticsearch.river.RiversModule

import de.kp.elastic.insight.module.{AnalyticsModule}

import de.kp.elastic.insight.rest._
import de.kp.elastic.insight.service.AnalyticsService

class AnalyticsPlugin(val settings:Settings) extends AbstractPlugin {
    
  override def name():String = {
    "elastic-insight"
  }

  override def description():String = {
    "Plugin that brings predictive analytics to Elasticsearch";
  }

  def onModule(module:RestModule) {
    
    /********************** METADATA MANAGEMENT ***********************/
    
    /*
     * A 'fields' request supports the retrieval of the field or metadata 
     * specificiations that are associated with a certain training task. 
     * The approach actually supported, enables the registration of field 
     * specifications on a per 'uid' basis, i.e. each task may have its own 
     * fields. Requests that have to refer to the same fields must provide 
     * the SAME 'uid'
     */
    module.addRestAction(classOf[FieldAction])
    /*
     * A 'register' request supports the registration of a field or metadata 
     * specification that describes the fields used to span the training dataset.
     */
   	module.addRestAction(classOf[RegisterAction])
    
    /********************** TRACKING MANAGEMENT ***********************/

    /*
     * 'index' and 'track' requests refer to the tracking functionality of
     * Predictiveworks; while 'index' prepares a certain Elasticsearch index, 
     * 'track' is used to gather training data.
     */
   	module.addRestAction(classOf[IndexAction])
    module.addRestAction(classOf[TrackAction])
    
    /********************** STATUS MANAGEMENT *************************/

    /*
     * A 'status' request supports the retrieval of the status with respect 
     * to a certain training task (uid). The latest status or all stati of a 
     * certain task are returned.
     */
   	module.addRestAction(classOf[StatusAction])
    
    /********************** MODEL MANAGEMENT **************************/
    /*
     * A 'params' request supports the retrieval of the parameters
     * used for a certain model training task
     */
    module.addRestAction(classOf[ParamAction])
    module.addRestAction(classOf[TrainAction])
    	
    /********************** INSIGHT API *******************************/

    module.addRestAction(classOf[GetAction])
    
  }

  /**
   * River API - not supported
   */
  def onModule(module:RiversModule) {
  }

  /**
   * Module API
   */
  override def modules():Collection[Class[_ <: Module]] = {

    val modules:Collection[Class[_ <: Module]] = Lists.newArrayList()
  	/* The module is bound to the AnalyticsService */
  	modules.add(classOf[AnalyticsModule])
        
    modules
    
  }

  /**
   * Service API
   */
  override def services():Collection[Class[_ <: LifecycleComponent[_]]] = {
    	
    val services:Collection[Class[_ <: LifecycleComponent[_]]] = Lists.newArrayList()
    services.add(classOf[AnalyticsService])        
      
    services
    
  }
	
}