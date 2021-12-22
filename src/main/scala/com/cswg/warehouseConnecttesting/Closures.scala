package com.cswg.testing


class Foo {
  def exec(f:(String) => Unit, name: String) {
      f(name)
  }
}
class Closures {
  
}

object Closures {
  var hello = "Hello"
  def sayHello(name: String) {
     println(s"$hello, $name")
  }
  
  def main (args: Array[String]){
      val foo = new Foo
      foo.exec(sayHello, "Pushkar")
      sayHello("Goyal")
      
      hello = "Hola"
      foo.exec(sayHello, "Pushkar")
      sayHello("Goyal")
  }
}