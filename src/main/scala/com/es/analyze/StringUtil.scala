package com.es.analyze

/**
  * Created by mick.yi on 2018/10/2.
  */
object StringUtil {
  /**
    * 是否是叠字
    * @param word
    * @return
    */
  def isReduplication(word:String)={
    word.length>1 && word.distinct.length==1
  }

}
