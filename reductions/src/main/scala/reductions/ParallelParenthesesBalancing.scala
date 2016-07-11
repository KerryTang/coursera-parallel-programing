package reductions

import scala.annotation._
import org.scalameter._
import common._
import scala.util.control.Breaks._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    } 
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")

    // val mb = 1024*1024
    // val runtime = Runtime.getRuntime
    // println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    // println("** Free Memory:  " + runtime.freeMemory / mb)
    // println("** Total Memory: " + runtime.totalMemory / mb)
    // println("** Max Memory:   " + runtime.maxMemory / mb)
    println(s"speedup: ${seqtime / fjtime}")
  
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    
    var x = 0
    var lPar = 0

    breakable{
      while (x < chars.length) {
        chars(x) match {
          case '(' => lPar += 1
          case ')' => lPar -= 1
          case _ => None
        }
        if (lPar < 0)
          break
        x += 1
      }
    }

    lPar == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    @tailrec
    def traverse(idx: Int, until: Int, unMatchLeft: Int, unMatchRight: Int): (Int, Int) = {
      if (idx >= until)
        (unMatchLeft, unMatchRight)
      else {
        val next = idx + 1
        val (l, r) = chars(idx) match {
          case '(' => (unMatchLeft+1, unMatchRight)
          case ')' => if (unMatchLeft > 0) (unMatchLeft-1, unMatchRight) else (unMatchLeft, unMatchRight+1)
          case _ => (unMatchLeft, unMatchRight)
        }
        traverse(next, until, l, r)
      }
    }

    def reduce(from: Int, until: Int):(Int, Int) = {
      if (until - from < threshold) {
        val (l, r) = traverse(from, until, 0, 0)
        // println("traverse:(%d, %d) -> (%d, %d)".format(from, until, l, r))
        (l, r)
      }
      else {
        val mid = (from + until)/2
        val ((ll, lr), (rl, rr)) = parallel(reduce(from, mid), reduce(mid, until))
        if (ll > rr) {
          // println("reduce(%d, %d) -> (%d, %d)".format(from, until, ll-rr+rl, lr))
          (ll-rr+rl, lr)
        }
        else {
          // println("reduce(%d, %d) -> (%d, %d)".format(from, until, rl, rr-ll+lr))
          (rl, rr-ll+lr)
        }
      }

    }
    reduce(0, chars.length) == (0, 0)
    
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
