package de.kp.elastic.insight.river
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

import java.util.Map

import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject

import org.elasticsearch.river.{AbstractRiverComponent,River,RiverName,RiverSettings}

import de.kp.elastic.insight.service.AnalyticsService
import de.kp.elastic.insight.river.handler.LearnHandler

import scala.collection.JavaConversions._

class AnalyticsRiver @Inject()(riverName: RiverName,settings: RiverSettings,client:Client,service:AnalyticsService) extends AbstractRiverComponent(riverName, settings) with River {
  
  /*
   * The request initiates the detection of outliers,
   * either by cluster analysis (KMeans) or by Markov
   */
  private val OUTLIERS = "outliers"
  /*
   * The request initiates the building of a rule-based
   * model, either through Top-K or Top-KNR association
   * rule mining
   */
  private val RULES = "rules"
  /*
   * The request initiates the building of a series-based
   * model, either through SPADE or TSR algorithm 
   */
  private val SERIES = "series"

  private val learner = new LearnHandler(settings,client)
  
  override def start() {
    
    logger.info("Start Analytics River")

    val props = settings.settings()
    val action = props("action").asInstanceOf[String]
    
    action match {
      
      case OUTLIERS => {
        
        learner.execute(action)
        logger.info("River started outlier detection.")
        
      }
      case RULES => {
        
        learner.execute(action)
        logger.info("River started rule discovery.")
      
      }
      case SERIES   => {
        
        learner.execute(action)
        logger.info("River started time series discovery.")
      
        
      }
      
      case _ => {
         logger.info("River {} has no actions. Deleting...", riverName.name())
      }
      
    }
   
  }

  override def close() {
    logger.info("Close ARules River");
  }
  
}