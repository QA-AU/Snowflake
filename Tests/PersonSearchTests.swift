import XCTest
@testable import SharpI

final class PersonSearchTests: XCTestCase {

    // MARK: - KnownPerson Model

    func testKnownPersonInit() {
        let person = KnownPerson(name: "Alice")
        XCTAssertEqual(person.name, "Alice")
        XCTAssertTrue(person.photoLocalIdentifiers.isEmpty)
        XCTAssertNil(person.faceEmbeddingData)
        XCTAssertNotNil(person.id)
        XCTAssertNotNil(person.createdAt)
    }

    func testKnownPersonAddIdentifiers() {
        let person = KnownPerson(name: "Bob")
        person.photoLocalIdentifiers = ["id1", "id2", "id3"]
        XCTAssertEqual(person.photoLocalIdentifiers.count, 3)
        XCTAssertTrue(person.photoLocalIdentifiers.contains("id2"))
    }

    func testKnownPersonEmbeddingStorage() {
        let person = KnownPerson(name: "Carol")
        let fakeEmbedding = Data([0x01, 0x02, 0x03])
        person.faceEmbeddingData = fakeEmbedding
        XCTAssertEqual(person.faceEmbeddingData, fakeEmbedding)
    }

    // MARK: - SearchQuery Person Mode

    func testPersonQueryMode() {
        let query = SearchQuery(text: "Alice", mode: .person)
        XCTAssertEqual(query.mode, .person)
        XCTAssertEqual(query.text, "Alice")
    }

    func testObjectQueryMode() {
        let query = SearchQuery(text: "red backpack", mode: .object)
        XCTAssertEqual(query.mode, .object)
    }

    func testQueryHasUniqueID() {
        let q1 = SearchQuery(text: "Alice", mode: .person)
        let q2 = SearchQuery(text: "Alice", mode: .person)
        XCTAssertNotEqual(q1.id, q2.id)
    }

    // MARK: - DateRange

    func testAllDateRangesExist() {
        let ranges = DateRange.allCases
        XCTAssertEqual(ranges.count, 4)
        XCTAssertTrue(ranges.contains(.today))
        XCTAssertTrue(ranges.contains(.thisWeek))
        XCTAssertTrue(ranges.contains(.thisMonth))
        XCTAssertTrue(ranges.contains(.allTime))
    }

    func testDateRangeRawValues() {
        XCTAssertEqual(DateRange.today.rawValue,     "Today")
        XCTAssertEqual(DateRange.thisWeek.rawValue,  "This Week")
        XCTAssertEqual(DateRange.thisMonth.rawValue, "This Month")
        XCTAssertEqual(DateRange.allTime.rawValue,   "All Time")
    }

    func testDateRangeIdentifiable() {
        XCTAssertEqual(DateRange.today.id, "Today")
    }
}
