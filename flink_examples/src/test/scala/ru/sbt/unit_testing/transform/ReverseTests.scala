package ru.sbt.unit_testing.transform

import TransformDataMain.reverse
import org.junit.Test
import org.junit.Assert._

class ReverseTests {
  @Test def testReverseEmpty(): Unit = {
    assertEquals("", reverse(""))
  }

  @Test def testReverseNonEmpty(): Unit = {
    assertEquals("tset", reverse("test"))
  }

  @Test def testReverseRussian(): Unit = {
    assertEquals("тсет", reverse("тест"))
  }

  @Test(expected = classOf[NullPointerException])
  def testReverseNull(): Unit = {
    assertEquals(null, reverse(null))
  }

  @Test def testReverseReverse(): Unit = {
    assertEquals(reverse("test"), reverse("test"))
  }

  @Test def testDoubleReverse(): Unit = {
    assertEquals("test", reverse(reverse("test")))
  }
}
