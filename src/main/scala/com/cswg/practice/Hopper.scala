package com.cswg.practice

class Solution {
  
}

object Solution {
  case class Heading(weight: Int, text: String)
  case class Node(heading: Heading, children: List[Node])

  def main(args: Array[String]) {
    val headings: Iterator[Heading] = scala.io.Source.stdin.getLines.flatMap(parse)
    val outline: Node = toOutline(headings)
    val html: String = toHtml(outline).trim
    println(html)
  }

  /** Converts a list of input headings into nested nodes */
  def toOutline(headings: Iterator[Heading]): Node = {
      import scala.collection.immutable.HashMap

    // Implement this function. Sample code below builds an
    // outline of only the first heading.
      def createNode(element: Node, siblings: List[Node] ): List[Node] = {
          return siblings.::(element)
      }
      
      val maintainChild: HashMap[Int, List[Node]] = new HashMap[Int, List[Node]]
      
      headings.foreach{ heading => 
            var emptyList: List[Node] = Nil
            if ( maintainChild.contains( heading.weight + 1 ) ){
               maintainChild.-(heading.weight + 1)
            }
            if ( maintainChild.contains( heading.weight - 1 ) ){
               emptyList = createNode( Node(heading, Nil) , maintainChild.getOrElse( heading.weight - 1, emptyList ) )
               maintainChild.+( (heading.weight - 1, emptyList) )
            }
            maintainChild.+( ( heading.weight, emptyList ) )
      }

    Node(Heading(0, ""), List(Node(headings.next, Nil)))
  }

  /** Parses a line of input.
    This implementation is correct for all predefined test cases. */
  def parse(record: String): Option[Heading] = {
    val H = "H(\\d+)".r
    record.split(" ", 2) match {
      case Array(H(level), text) =>
        scala.util.Try(Heading(level.toInt, text.trim)).toOption
      case _ => None
    }
  }

  /** Converts a node to HTML.
    This implementation is correct for all predefined test cases. */
  def toHtml(node: Node): String = {
    val childHtml = node.children match {
      case Nil => ""
      case children =>
        "<ol>" + children.map(
          child => "<li>" + toHtml(child) + "</li>"
        ).mkString("\n") + "</ol>"
    }
    val heading =
      if (node.heading.text.isEmpty) ""
      else node.heading.text + "\n"
    heading + childHtml
  }
}
