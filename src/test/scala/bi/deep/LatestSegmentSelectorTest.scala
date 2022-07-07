package bi.deep

import bi.deep.LatestSegmentSelector.listPrimaryPartitions
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate

class LatestSegmentSelectorTest extends AnyFunSuite {

  test("Correctly return path when List of dates is not empty") {
    val path: String = "hdfs://test/"
    val dates = List((LocalDate.parse("2022-01-01"), LocalDate.parse("2022-01-02")), (LocalDate.parse("2022-01-02"), LocalDate.parse("2022-01-03")))
    val result = listPrimaryPartitions(dates, path)
    val expected = List("hdfs://test/2022-01-01T00_00_00.000Z_2022-01-02T00_00_00.000Z", "hdfs://test/2022-01-02T00_00_00.000Z_2022-01-03T00_00_00.000Z")
    assertResult(expected)(result)
  }

  test("Empty List of dates") {
    val path: String = "hdfs://test/"
    val dates = List.empty
    val result = listPrimaryPartitions(dates, path)
    val expected = List.empty
    assertResult(expected)(result)
  }


  test("Correctly return one-element list of (LocalDate, LocalDate) when startDate - endDate equals 1 day") {
    val startDate: LocalDate = LocalDate.parse("2022-01-01")
    val endDate: LocalDate = LocalDate.parse("2022-01-02")
    val result = LatestSegmentSelector.getIntervals(startDate, endDate)
    val expected = List((startDate, endDate))
    assertResult(expected)(result)
  }

  test("Correctly return an empty list when endDate > startDate") {
    val startDate: LocalDate = LocalDate.parse("2022-01-10")
    val endDate: LocalDate = LocalDate.parse("2022-01-05")
    val result = LatestSegmentSelector.getIntervals(startDate, endDate)
    val expected = List.empty
    assertResult(expected)(result)
  }

  test("Correctly return an empty list when endDate = startDate") {
    val startDate: LocalDate = LocalDate.parse("2022-01-10")
    val endDate: LocalDate = LocalDate.parse("2022-01-10")
    val result = LatestSegmentSelector.getIntervals(startDate, endDate)
    val expected = List.empty
    assertResult(expected)(result)
  }

  test("Correctly return a list of (LocalDate, LocalDate)") {
    val startDate: LocalDate = LocalDate.parse("2022-01-05")
    val endDate: LocalDate = LocalDate.parse("2022-01-10")
    val result = LatestSegmentSelector.getIntervals(startDate, endDate)
    val expected = List("2022-01-05", "2022-01-06", "2022-01-07", "2022-01-08", "2022-01-09").map(LocalDate.parse)
      .map(date => date -> date.plusDays(1))
    assertResult(expected)(result)
  }

  test("Correctly add day to LocalDate") {
    val startDate: LocalDate = LocalDate.parse("2022-01-05")
    val result = LocalDate.parse("2022-01-06")
    val expected = startDate.plusDays(1)
    assertResult(expected)(result)
  }
}
