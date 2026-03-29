import XCTest
@testable import SpotSpeedCam

final class AppSettingsTests: XCTestCase {

    override func setUp() {
        super.setUp()
        // Clear UserDefaults before each test
        let keys = ["audioAlertEnabled", "confidenceThreshold", "frameSkip",
                    "cooldownSeconds", "clipWarningDismissed"]
        keys.forEach { UserDefaults.standard.removeObject(forKey: $0) }
    }

    // MARK: - Default values

    func test_defaults_audioAlertEnabled_isTrue() {
        XCTAssertTrue(AppSettings().audioAlertEnabled)
    }

    func test_defaults_confidenceThreshold_is60percent() {
        XCTAssertEqual(AppSettings().confidenceThreshold, 0.60, accuracy: 1e-5)
    }

    func test_defaults_frameSkip_is5() {
        XCTAssertEqual(AppSettings().frameSkip, 5)
    }

    func test_defaults_cooldownSeconds_is8() {
        XCTAssertEqual(AppSettings().cooldownSeconds, 8.0, accuracy: 1e-5)
    }

    func test_defaults_clipWarningDismissed_isFalse() {
        XCTAssertFalse(AppSettings().clipWarningDismissed)
    }

    // MARK: - Persistence

    func test_audioAlertEnabled_persists() {
        let settings = AppSettings()
        settings.audioAlertEnabled = false
        XCTAssertFalse(AppSettings().audioAlertEnabled)
    }

    func test_confidenceThreshold_persists() {
        let settings = AppSettings()
        settings.confidenceThreshold = 0.80
        XCTAssertEqual(AppSettings().confidenceThreshold, 0.80, accuracy: 1e-5)
    }

    func test_frameSkip_persists() {
        let settings = AppSettings()
        settings.frameSkip = 10
        XCTAssertEqual(AppSettings().frameSkip, 10)
    }

    // MARK: - Keychain round-trip

    func test_keychain_saveAndLoad() {
        let settings = AppSettings()
        let testKey = "hf_test_key_abc123"

        settings.saveHuggingFaceKey(testKey)
        let loaded = AppSettings.loadHuggingFaceKey()
        XCTAssertEqual(loaded, testKey)

        settings.deleteHuggingFaceKey()   // Clean up
    }

    func test_keychain_deleteRemovesKey() {
        let settings = AppSettings()
        settings.saveHuggingFaceKey("some_key")
        settings.deleteHuggingFaceKey()
        XCTAssertNil(AppSettings.loadHuggingFaceKey())
    }

    func test_keychain_loadReturnsNilIfAbsent() {
        AppSettings().deleteHuggingFaceKey()
        XCTAssertNil(AppSettings.loadHuggingFaceKey())
    }
}
