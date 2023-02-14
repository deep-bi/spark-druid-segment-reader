package bi.deep.segments

import bi.deep.segments.MixedGranularitySolver.{Overlapping, solve}
import org.joda.time.Interval
import org.scalatest.funsuite.AnyFunSuite

class MixedGranularitySolverTest extends AnyFunSuite {

  case class Foo(interval: Interval, priority: Int)

  object Foo {
    def apply(start: Long, end: Long, priority: Int): Foo = new Foo(new Interval(start, end), priority)
  }

  private implicit val overlapping: Overlapping[Foo] = new Overlapping[Foo] {
    /** Defines if two values overlaps (have common part) */
    override def overlaps(left: Foo, right: Foo): Boolean = left.interval.overlaps(right.interval)
  }

  private implicit val ordering: Ordering[Foo] = Ordering.fromLessThan((l, r) => Ordering.Int.lt(l.priority, r.priority))

  test("solver without overlaps") {
    val values = List(Foo(0, 1, 1), Foo(1, 2, 1), Foo(2, 3, 1), Foo(3, 4, 1))

    val results = solve(values)

    assertResult(values.toSet)(results.toSet)
  }

  test("solver with single overlap - decides ordering") {
    val values = List(Foo(0, 1, 1), Foo(0, 1, 10))
    val result = solve(values)

    assertResult(List(Foo(0, 1, 10)))(result)
  }

  /**
   * 0     1     2
   * | A 1 | B 2 |
   * | C 3       |
   * */
  test("longer is greater then overlaps - 2") {
    val a = Foo(0, 1, 1)
    val b = Foo(1, 2, 2)
    val c = Foo(0, 2, 3)

    val result = solve(List(a, b, c)).toSet
    assertResult(Set(c))(result)
  }

  /**
   * 0     1     2     3
   * | A 1 | B 2 | D 3 |
   * | C 4             |
   * */
  test("longer is greater then overlaps - 3") {
    val a = Foo(0, 1, 1)
    val b = Foo(1, 2, 2)
    val c = Foo(0, 3, 4)
    val d = Foo(2, 3, 3)

    val result = solve(List(a, b, c, d)).toSet
    assertResult(Set(c))(result)
  }

  /**
   * 0     1     2
   * | A 1       |
   * | B 2 | C 3 |
   */
  test("shorts are greater then overlap - 2") {
    val a = Foo(0, 2, 1)
    val b = Foo(0, 1, 2)
    val c = Foo(1, 2, 3)

    val result = solve(List(a, b, c)).toSet
    assertResult(Set(b, c))(result)
  }

  /**
   * 0     1     2     3     4
   * | A 1 | B 2 | C 3 | E 5 |
   * | D 4       |
   */
  test("shorts are greater then overlap - 2, mix") {
    val a = Foo(0, 1, 1)
    val b = Foo(1, 2, 2)
    val c = Foo(2, 3, 3)
    val d = Foo(0, 2, 4)
    val e = Foo(3, 4, 5)


    val result = solve(List(a, b, c, d, e)).toSet
    assertResult(Set(d, c, e))(result)
  }

}
