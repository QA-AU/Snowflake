import XCTest
@testable import SharpI

final class iCloudAssetTests: XCTestCase {

    // MARK: - AppSettings iCloud

    func testICloudDefaultsToFalse() {
        UserDefaults.standard.removeObject(forKey: "iCloudSyncEnabled")
        let settings = AppSettings()
        XCTAssertFalse(settings.iCloudSyncEnabled)
    }

    func testICloudCanBeEnabled() {
        let settings = AppSettings()
        settings.iCloudSyncEnabled = true
        XCTAssertTrue(settings.iCloudSyncEnabled)
        settings.iCloudSyncEnabled = false  // cleanup
    }

    func testICloudPersistedAcrossInstances() {
        let s1 = AppSettings()
        s1.iCloudSyncEnabled = true
        let s2 = AppSettings()
        XCTAssertTrue(s2.iCloudSyncEnabled)
        s1.iCloudSyncEnabled = false  // cleanup
    }

    // MARK: - SearchSession

    func testSearchSessionInit() {
        let session = SearchSession()
        XCTAssertTrue(session.queries.isEmpty)
        XCTAssertTrue(session.results.isEmpty)
        XCTAssertNil(session.endedAt)
        XCTAssertNotNil(session.startedAt)
    }

    func testSearchSessionCanEnd() {
        let session = SearchSession()
        let now = Date()
        session.endedAt = now
        XCTAssertNotNil(session.endedAt)
    }

    func testSearchSessionHasUniqueID() {
        let s1 = SearchSession()
        let s2 = SearchSession()
        XCTAssertNotEqual(s1.id, s2.id)
    }

    // MARK: - KnownItem

    func testKnownItemInit() {
        let item = KnownItem(name: "Red backpack")
        XCTAssertEqual(item.name, "Red backpack")
        XCTAssertNil(item.embeddingData)
        XCTAssertNil(item.lastSeenAt)
    }

    func testKnownItemCanStoreEmbedding() {
        let item = KnownItem(name: "Keys")
        item.embeddingData = Data([0xFF, 0x00])
        XCTAssertNotNil(item.embeddingData)
    }
}
