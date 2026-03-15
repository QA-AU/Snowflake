import XCTest
@testable import SharpI

final class ThermalAndSettingsTests: XCTestCase {

    // MARK: - ThermalStateManager

    @MainActor
    func testThermalManagerInit() {
        let manager = ThermalStateManager()
        XCTAssertNotNil(manager.currentState)
    }

    @MainActor
    func testThermalNominalNotCritical() {
        let manager = ThermalStateManager()
        // In CI / simulator the device is almost always nominal.
        if ProcessInfo.processInfo.thermalState == .nominal {
            XCTAssertFalse(manager.isCritical)
            XCTAssertFalse(manager.isSerious)
        }
    }

    // MARK: - AppSettings Defaults

    func testVoiceReadoutDefaultTrue() {
        UserDefaults.standard.removeObject(forKey: "voiceReadoutEnabled")
        let settings = AppSettings()
        XCTAssertTrue(settings.voiceReadoutEnabled)
    }

    func testAmberReadoutDefaultFalse() {
        UserDefaults.standard.removeObject(forKey: "amberReadoutEnabled")
        let settings = AppSettings()
        XCTAssertFalse(settings.amberReadoutEnabled)
    }

    func testVoiceReadoutPersists() {
        let s1 = AppSettings()
        s1.voiceReadoutEnabled = false
        let s2 = AppSettings()
        XCTAssertFalse(s2.voiceReadoutEnabled)
        s1.voiceReadoutEnabled = true  // cleanup
    }

    // MARK: - API Key Keychain Round-Trip

    func testAPIKeyRoundTrip() {
        let settings = AppSettings()
        let testKey  = "hf_test_\(UUID().uuidString)"
        settings.huggingFaceAPIKey = testKey
        let reloaded = AppSettings()
        XCTAssertEqual(reloaded.huggingFaceAPIKey, testKey)
        settings.huggingFaceAPIKey = ""  // cleanup
    }

    func testAPIKeyDeletedWhenEmpty() {
        let settings = AppSettings()
        settings.huggingFaceAPIKey = "hf_temp"
        settings.huggingFaceAPIKey = ""
        let reloaded = AppSettings()
        XCTAssertEqual(reloaded.huggingFaceAPIKey, "")
    }

    // MARK: - ConfidenceTier

    func testTierReadoutGreen()  { XCTAssertTrue(ConfidenceTier.green.readoutEnabled) }
    func testTierReadoutYellow() { XCTAssertTrue(ConfidenceTier.yellow.readoutEnabled) }
    func testTierReadoutAmber()  { XCTAssertFalse(ConfidenceTier.amber.readoutEnabled) }

    func testTierColorValues() {
        XCTAssertEqual(ConfidenceTier.green.color,  "#00C853")
        XCTAssertEqual(ConfidenceTier.yellow.color, "#FFD600")
        XCTAssertEqual(ConfidenceTier.amber.color,  "#FF6D00")
    }

    func testTierInitFromConfidence() {
        XCTAssertEqual(ConfidenceTier(confidence: 0.90), .green)
        XCTAssertEqual(ConfidenceTier(confidence: 0.85), .yellow)  // not strictly > 0.85
        XCTAssertEqual(ConfidenceTier(confidence: 0.60), .yellow)
        XCTAssertEqual(ConfidenceTier(confidence: 0.55), .amber)
        XCTAssertEqual(ConfidenceTier(confidence: 0.10), .amber)
    }
}
