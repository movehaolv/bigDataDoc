//package chapter09plus
//
///**
// * @Author: lvhao-004
// * @Version: 1.0
// * @Date: Create in 13:21 2021/7/7
// */
//
//
//object Test  {
//  implicit val str: String = "hello world!"
//  implicit val str1: String = "hell111o world!"
//  def hello(implicit arg: String="good bey world!"): Unit = {
//    println(arg)
//  }
//  def main(args: Array[String]): Unit = {
//    hello
//  }
//}
//
//class MyRicherInt(self: Int){
//  def myMin(n: Int) = if(n < self) n else self
//  def myMax(n: Int) = if(n < self) self else n
//}
//
//object MyRicherInt{
//  def apply(n: Int) = new MyRichInt(n)
//}
//
