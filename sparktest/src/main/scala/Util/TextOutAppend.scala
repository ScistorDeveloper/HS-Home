package Util

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
  * Created by Administrator on 2017/11/16.
  */
class TextOutAppend extends MultipleTextOutputFormat[Any,Any]{

  def showCapital(key: String) = key match {
    case key => key
    case "" => "no.host.exist"
    case key => "no.host.exist"
  }

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String):
  String = "host." + key.asInstanceOf[String]
}
