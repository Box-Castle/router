package com.box.castle.collections.immutable

import org.specs2.matcher.MatchResult
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

class LinkedHashMapTest extends Specification with Mockito {

  "LinkedHashMap" should {

    "return keys in insertion order" in {
      LinkedHashMap(1 -> "a").keys must_== List(1)

      LinkedHashMap(1 -> "a", 2 -> "b").keys must_== List(1, 2)
      LinkedHashMap(2 -> "b", 1 -> "a").keys must_== List(2, 1)

      LinkedHashMap(1 -> "a", 2 -> "b", 3 -> "c").keys must_== List(1, 2, 3)
      LinkedHashMap(1 -> "a", 3 -> "c", 2 -> "b").keys must_== List(1, 3, 2)
      LinkedHashMap(2 -> "b", 1 -> "a", 3 -> "c").keys must_== List(2, 1, 3)
      LinkedHashMap(2 -> "b", 3 -> "c", 1 -> "a").keys must_== List(2, 3, 1)
      LinkedHashMap(3 -> "c", 1 -> "a", 2 -> "b").keys must_== List(3, 1, 2)
      LinkedHashMap(3 -> "c", 2 -> "b", 1 -> "a").keys must_== List(3, 2, 1)
    }

    "return values in insertion order" in {
      LinkedHashMap(1 -> "a").values must_== List("a")

      LinkedHashMap(1 -> "a", 2 -> "b").values must_== List("a", "b")
      LinkedHashMap(2 -> "b", 1 -> "a").values must_== List("b", "a")

      LinkedHashMap(1 -> "a", 2 -> "b", 3 -> "c").values must_== List("a", "b", "c")
      LinkedHashMap(1 -> "a", 3 -> "c", 2 -> "b").values must_== List("a", "c", "b")
      LinkedHashMap(2 -> "b", 1 -> "a", 3 -> "c").values must_== List("b", "a", "c")
      LinkedHashMap(2 -> "b", 3 -> "c", 1 -> "a").values must_== List("b", "c", "a")
      LinkedHashMap(3 -> "c", 1 -> "a", 2 -> "b").values must_== List("c", "a", "b")
      LinkedHashMap(3 -> "c", 2 -> "b", 1 -> "a").values must_== List("c", "b", "a")
    }

    "preserve withDefaultValue through modifications" in {
      var m = LinkedHashMap.empty[Int, String].withDefaultValue("default")
      m(33235) must_== "default"
      m(121353) must_== "default"

      m = m + (245 -> "abc")
      m(245) must_== "abc"
      m(111) must_== "default"

      m = m - 245
      m(245) must_== "default"
      m(111) must_== "default"

      m = m + (943 -> "xyz")
      m(943) must_== "xyz"
      m(13432411) must_== "default"

      val (k, v, newM) = m.removeHead()
      newM(83216) must_== "default"
      m = newM
      k must_== 943
      v must_== "xyz"
      m(8257) must_== "default"

      m.isEmpty must_== true
      m must_== LinkedHashMap.empty[Int, String]

      val emptyM = m.empty
      emptyM(235235) must_== "default"

      m = m + (987 -> "zzzz")
      m(22222) must_== "default"
      m(987) must_== "zzzz"

      m = m.updated(987, "yyyy")
      m(22222) must_== "default"
      m(987) must_== "yyyy"
      m(765) must_== "default"

      m = m.updated(765, "qq")
      m(22222) must_== "default"
      m(987) must_== "yyyy"
      m(765) must_== "qq"
    }

    "return values in insertion order when iterating" in {
      def verify(m: LinkedHashMap[Int, String], expected: Vector[(Int, String)]): MatchResult[Any] = {
        var i = 0
        m.foreach {
          case (k, v) => {
            expected(i) must_== ((k, v))
            i += 1
          }
        }
        expected.size must_== i
      }
      verify(LinkedHashMap(1 -> "a"), Vector((1, "a")))


      verify(LinkedHashMap(1 -> "a", 2 -> "b"), Vector((1, "a"), (2, "b")))
      verify(LinkedHashMap(2 -> "b", 1 -> "a"), Vector((2, "b"), (1, "a")))

      verify(LinkedHashMap(1 -> "a", 2 -> "b", 3 -> "c"), Vector((1, "a"), (2, "b"), (3, "c")))
      verify(LinkedHashMap(1 -> "a", 3 -> "c", 2 -> "b"), Vector((1, "a"), (3, "c"), (2, "b")))
      verify(LinkedHashMap(2 -> "b", 1 -> "a", 3 -> "c"), Vector((2, "b"), (1, "a"), (3, "c")))
      verify(LinkedHashMap(2 -> "b", 3 -> "c", 1 -> "a"), Vector((2, "b"), (3, "c"), (1, "a")))
      verify(LinkedHashMap(3 -> "c", 1 -> "a", 2 -> "b"), Vector((3, "c"), (1, "a"), (2, "b")))
      verify(LinkedHashMap(3 -> "c", 2 -> "b", 1 -> "a"), Vector((3, "c"), (2, "b"), (1, "a")))
    }

    "correctly return isEmpty when adding/removing items" in {
      var m = LinkedHashMap.empty[Int, String]
      m.isEmpty must_== true

      m = m + (1 -> "a")
      m.isEmpty must_== false

      m = m - 1
      m.isEmpty must_== true

      m = m + (2 -> "b")
      m.isEmpty must_== false

      val (k, v, newM) = m.removeHead()
      k must_== 2
      v must_== "b"
      newM.isEmpty must_== true
    }

    "correctly work on get operations" in {
      val m = LinkedHashMap(1 -> "a", 2 -> "b", 3 -> "c")
      m.get(1) must_== Some("a")
      m.get(2) must_== Some("b")
      m.get(3) must_== Some("c")
      m.get(4) must_== None

      // Works ok on empty
      LinkedHashMap.empty[Int, String].get(1) must_== None
    }

    "correctly work on apply operations" in {
      val m = LinkedHashMap(1 -> "a", 2 -> "b", 3 -> "c")
      m(1) must_== "a"
      m(2) must_== "b"
      m(3) must_== "c"

      m(4) must throwA(new NoSuchElementException("key not found: 4"))

      // Works ok on empty
      LinkedHashMap.empty[Int, String](55) must throwA(new NoSuchElementException("key not found: 55"))
    }

    "raise an exception when head is attempted while empty" in {
      LinkedHashMap.empty[Any, Any].head must
        throwA(new NoSuchElementException("head on empty LinkedHashMap"))
    }

    "raise an exception when last is attempted while empty" in {
      LinkedHashMap.empty[Any, Any].last must
        throwA(new NoSuchElementException("last on empty LinkedHashMap"))
    }

    "raise an exception when head is attempted while empty" in {
      LinkedHashMap.empty[Any, Any].head must
        throwA(new NoSuchElementException("head on empty LinkedHashMap"))
    }

    "return None for headOption/lastOption when empty" in {
      LinkedHashMap.empty[Any, Any].headOption must_== None
      LinkedHashMap.empty[Any, Any].lastOption must_== None
    }

    "raise an exception when removeHead is attempted while empty" in {
      LinkedHashMap.empty[Any, Any].removeHead must
        throwA(new NoSuchElementException("removeHead on empty LinkedHashMap"))
    }

    "properly removeHead" in {
      val m = LinkedHashMap(2 -> "b", 1 -> "c")

      val (k, v, newM) = m.removeHead()
      k must_== 2
      v must_== "b"
      newM.isEmpty must_== false

      val (k2, v2, newM2) = newM.removeHead()
      k2 must_== 1
      v2 must_== "c"
      newM2.isEmpty must_== true
    }

    "return correct head/last when adding items and using removeHead" in {
      var m = LinkedHashMap(3 -> "x")
      m.head must_== ((3, "x"))
      m.headOption must_== Some((3, "x"))
      m.last must_== ((3, "x"))
      m.lastOption must_== Some((3, "x"))

      m = m + (2 -> "y")
      m.head must_== ((3, "x"))
      m.headOption must_== Some((3, "x"))
      m.last must_== ((2, "y"))
      m.lastOption must_== Some((2, "y"))

      m = m + (1 -> "z")
      m.head must_== ((3, "x"))
      m.headOption must_== Some((3, "x"))
      m.last must_== ((1, "z"))
      m.lastOption must_== Some((1, "z"))

      // Start removing
      val (k, v, newM) = m.removeHead()
      k must_== 3
      v must_== "x"
      m = newM
      m.head must_== ((2, "y"))
      m.headOption must_== Some((2, "y"))
      m.last must_== ((1, "z"))
      m.lastOption must_== Some((1, "z"))

      val (k2, v2, newM2) = m.removeHead()
      k2 must_== 2
      v2 must_== "y"
      m = newM2
      m.head must_== ((1, "z"))
      m.headOption must_== Some((1, "z"))
      m.last must_== ((1, "z"))
      m.lastOption must_== Some((1, "z"))

      val (k3, v3, newM3) = m.removeHead()
      k3 must_== 1
      v3 must_== "z"
      m = newM3
      m.headOption must_== None
      m.lastOption must_== None
    }

    "removing an element that does not exist returns back the same LinkedHashMap" in {
      val m = LinkedHashMap(3 -> "x")
      m eq (m - 3235928)
    }

    "ensure that equals/hashcode takes into account insertion order" in {
      val m = LinkedHashMap(3 -> "x")
      val m2 = LinkedHashMap(3 -> "x")
      (m == m2) must_== true
      m.hashCode() must_== m2.hashCode()

      val m3 = LinkedHashMap(3 -> "x", 2 -> "y")
      val m4 = LinkedHashMap(3 -> "x", 2 -> "y")
      (m3 == m4) must_== true
      m3.hashCode() must_== m4.hashCode()

      val m5 = LinkedHashMap(3 -> "x", 2 -> "y")
      val m6 = LinkedHashMap(2 -> "y", 3 -> "x")
      (m5 == m6) must_== false
      m5.hashCode() must_!= m6.hashCode()

      (m5 - 3 == (m6 - 3)) must_== true
      (m5 - 3).hashCode() must_== (m6 - 3).hashCode()
    }

    "return correct head/last when adding items and removing via -" in {
      var m = LinkedHashMap(3 -> "x")
      m.head must_== ((3, "x"))
      m.headOption must_== Some((3, "x"))
      m.last must_== ((3, "x"))
      m.lastOption must_== Some((3, "x"))

      m = m + (2 -> "y")
      m.head must_== ((3, "x"))
      m.headOption must_== Some((3, "x"))
      m.last must_== ((2, "y"))
      m.lastOption must_== Some((2, "y"))

      m = m + (1 -> "z")
      m.head must_== ((3, "x"))
      m.headOption must_== Some((3, "x"))
      m.last must_== ((1, "z"))
      m.lastOption must_== Some((1, "z"))

      // Cover removal of the head element
      val m1 = m - 3
      m1.head must_== ((2, "y"))
      m1.headOption must_== Some((2, "y"))
      m1.last must_== ((1, "z"))
      m1.lastOption must_== Some((1, "z"))
      m1.size must_== 2

      // Cover removal of the middle element
      val m2 = m - 2
      m2.head must_== ((3, "x"))
      m2.headOption must_== Some((3, "x"))
      m2.last must_== ((1, "z"))
      m2.lastOption must_== Some((1, "z"))
      m2.size must_== 2

      // Cover removal of the last element
      val m3 = m - 1
      m3.head must_== ((3, "x"))
      m3.headOption must_== Some((3, "x"))
      m3.last must_== ((2, "y"))
      m3.lastOption must_== Some((2, "y"))
      m3.size must_== 2
    }

    "have a reasonable toString" in {
      LinkedHashMap(1 -> "a", 2 -> "b").toString() must_==
        "LinkedHashMap(1 -> a, 2 -> b)"
    }

    "not have a degenerate equals" in {
      val x: Any = LinkedHashMap(1 -> "some string")
      x.equals("hello") must_== false

      val y: Any = LinkedHashMap()
      y.equals("hello") must_== false

      val z: Any = LinkedHashMap(1 -> "some string").empty
      z.equals("hello") must_== false

      LinkedHashMap().equals(LinkedHashMap.empty) must_== true
    }

    "have a working withDefault" in {
      val x = LinkedHashMap.empty[Int, String].withDefault((x) => x.toString + "zzz")
      x(35) must_== "35zzz"
      x(11) must_== "11zzz"
    }

    "updating existing key does not affect insertion order" in {
      var x = LinkedHashMap.empty[Int, String]
      x = x + (1 -> "a")
      x = x + (2 -> "b")
      x = x + (3 -> "c")
      x = x + (4 -> "d")
      x = x + (5 -> "e")

      x = x.updated(5, "EEE")
      x = x.updated(3, "CCC")
      x = x.updated(1, "AAA")

      x = x.updated(6, "qqq")

      var (k1, v1, newX1) = x.removeHead()
      x = newX1
      k1 must_== 1
      v1 must_== "AAA"

      var (k2, v2, newX2) = x.removeHead()
      x = newX2
      k2 must_== 2
      v2 must_== "b"

      var (k3, v3, newX3) = x.removeHead()
      x = newX3
      k3 must_== 3
      v3 must_== "CCC"

      var (k4, v4, newX4) = x.removeHead()
      x = newX4
      k4 must_== 4
      v4 must_== "d"

      var (k5, v5, newX5) = x.removeHead()
      x = newX5
      k5 must_== 5
      v5 must_== "EEE"

      var (k6, v6, newX6) = x.removeHead()
      x = newX6
      k6 must_== 6
      v6 must_== "qqq"

    }
  }
}