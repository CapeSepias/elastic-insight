package de.kp.elastic.insight.utils
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
import scala.collection.JavaConversions._

object StringUtils {

  /**
   * Check if a collection of a string is empty.
   */
  def isEmpty(c:Collection[String]):Boolean = {
    if (c == null || c.isEmpty()) {
      return false
    }
        
    for (text <- c) {
      if (isNotEmpty(text)) {
        return false            
      }        
    }
    
    return true
    
  }

  /**
   * Check if a collection of a string is not empty.
   */
  def isNotEmpty(c:Collection[String]):Boolean = {
    return !isEmpty(c)
  }

  /**
   * Check if a collection of a string is blank.
   */
  def isBlank(c:Collection[String]):Boolean = {
    if (c == null || c.isEmpty()) {
      return false
    }
    
    for (text <- c) {
      if (isNotBlank(text)) {
        return false        
      }      
    }
    
    return true
    
  }

  /**
   * Check if a collection of a string is not blank.
   */
  def isNotBlank(c:Collection[String]):Boolean = {
    !isBlank(c)
  }

  /**
   * Check if a string is empty.
   */
  def isEmpty(text:String):Boolean = {
    text == null || text.length() == 0
  }

  /**
   * Check if a string is not empty.
   *
   */
  def isNotEmpty(text:String):Boolean = {
    !isEmpty(text)
  }

  /**
   * Check if a string is blank.
   */
  def isBlank(str:String):Boolean = {
    if (str == null || str.length() == 0) {
      return true
     }
     
    for (i <- 0 until str.length()) {
      if (!Character.isWhitespace(str.charAt(i))) {
        return false
      }
      
    }
    
    return true
    
  }

  /**
   * Check if a string is not blank.
   */
  def isNotBlank(str:String):Boolean = {
    return !isBlank(str)
  }

}
