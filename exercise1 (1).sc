import scala.collection.mutable.ListBuffer


// task 1 solution

def mySum(x: Int, y: Int): Int= {
  x+y

}
val s = mySum(20, 21)

// task 1 end

// task 2 solution


def pascalTriangle(col: Int, row: Int): Int = {
  if (col == 0 || (col == row)) 1
  else
    pascalTriangle(col-1, row-1) + pascalTriangle(col, row - 1)
}
 pascalTriangle(4,5)

// task 2 end

// task3 solution

def balance(chars: List[Char]): Boolean = {

  //true for paren false for other chars
  def isParen(aChar : Char) : Boolean = {
    if (aChar == '(' || aChar == ')')
      true
    else
      false
  }

  def subBalanced(chars: List[Char]) : Boolean = {
    //if we have an empty list we're technically balanced
    if (chars.isEmpty) true
    //if an open paren is at the beginning we aren't balanced
    else if (chars.head == ')') false
    //if we have two of the same type of parentheses we aren't balanced
    else if (chars.head == chars.last)
      false
    else subBalanced(chars.drop(1).dropRight(1))
  }

  //runs recursive function on filtered list
  subBalanced(chars.filter(isParen _))
}
balance(List('a', '(', ')'))
balance("a()".toList)



// task 4 solution

var a = Array(1, 2, 3, 4, 5)

var b = a.map(a => a * a);
val result= b.reduceLeft(_+_)

// task 4 end

// task 5 solution
// split method break the sentence into single word,
"sheena is a punk rocker she is a punk punk".split(" ")

// then Map is an Iterable consisting of pairs of keys and values,
"sheena is a punk rocker she is a punk punk".split(" ").map(s => (s, 1))
//then groupBy function takes a predicate as a parameter and based on this it group our elements into a useful key value pair map

"sheena is a punk rocker she is a punk punk".split(" ").map(s => (s, 1)).groupBy(p => p._1)

// the last line will count the how many element are there with their key and sum up the length.

"sheena is a punk rocker she is a punk punk".split(" ").map(s => (s, 1)).groupBy(p => p._1).map({case(v1, v2) => (v1, v2.length)})

"sheena is a punk rocker she is a punk punk".split(" " ).map((_, 1)).groupBy(_._1).map({case(v1, v2) => (v1, v2.map(_._2).reduce(_+_))})
//
//
//  v2.map(_._2) emits a second component of the tuple
// then reduces function to reduce the collection data structure in Scala. This function can be applied for both mutable and immutable collection data structure
// task 5 end


// task 6 solution
def  check(epsilon: Double, s: Stream[Double]): Double = s match {
  case x0 #:: x1 #:: xs => if (math.abs(x0 - x1) < epsilon) x1 else check(epsilon, x1 #:: xs)
}

def sqrt(n: Double)(implicit epsilon: Double): Double = {
  require(n >= 0)
  def next(x0: Double) = (x0 + n / x0) / 2
  val initial = n / 2

  check(epsilon, Stream.iterate(initial)(next))
}

implicit val epsilon = 0.1

sqrt(2)
sqrt(100)
sqrt(1e-16)
sqrt(1e60)

// task 6 end

