package chapter09plus

/**
 * @Author: lvhao-004
 * @Version: 1.0
 * @Date: Create in 13:21 2021/7/7 
 */


object Test  {
  def main(args: Array[String]): Unit = {
      // 1.
    implicit def convert(n: Int) = MyRicherInt(n)

      // 2.
    implicit val m: String = "ccc"

    def getParams(ars: String = "aaaaa") = {
      println(ars)
      println(implicitly[String])
    }
    getParams("asd")

    implicit class MyRicherInt2(self: Int){
      def myMin2(n: Int) = if(n < self) n + 100 else self
      def myMax(n: Int) = if(n < self) self + 100 else n
    }

    println(12.myMin2(2))
  }
}

class MyRicherInt(self: Int){
  def myMin(n: Int) = if(n < self) n else self
  def myMax(n: Int) = if(n < self) self else n
}

object MyRicherInt{
  def apply(n: Int) = new MyRichInt(n)
}

