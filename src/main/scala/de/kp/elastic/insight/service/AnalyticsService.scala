package de.kp.elastic.insight.service
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

import org.elasticsearch.ElasticsearchException
import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings

class AnalyticsService @Inject() (settings:Settings) extends AbstractLifecycleComponent[AnalyticsService](settings) {

  logger.info("Create Analytics Service")

  @Override
  protected def doStart() {
    	
    logger.info("Start Analytics Service")
    
  }

  @Override
  protected def doStop() {
        
    logger.info("Stop Analytics Service")
    
  }

  @Override
  protected def doClose() {
    
    logger.info("Close Analytics Service")
    
  }

}