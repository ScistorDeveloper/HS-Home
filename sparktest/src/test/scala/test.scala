import Util.UrlCheck

/**
  * Created by Administrator on 2017/11/16.
  */
object test {
  def main(args: Array[String]): Unit = {
    print(UrlCheck.getCheck.checkUrl("www.baidu.com"))
    print("test")
    print("".equals(""))
  }
}
