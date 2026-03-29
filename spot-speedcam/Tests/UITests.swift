import XCTest

// MARK: - UITests
// Run with: Product → Test (⌘U) on a real device or simulator with camera permission pre-granted.

final class UITests: XCTestCase {

    var app: XCUIApplication!

    override func setUpWithError() throws {
        continueAfterFailure = false
        app = XCUIApplication()
        // Grant camera permission via launch args (requires SetUpLaunchArguments approach)
        app.launchArguments = ["-UITesting", "-GrantCameraPermission"]
        app.launch()
    }

    // MARK: - Launch

    func test_appLaunches_showsCameraView() {
        // The scan toggle button should be visible on the main screen
        XCTAssertTrue(app.buttons["Start scanning"].waitForExistence(timeout: 5))
    }

    // MARK: - Scan toggle

    func test_tapScanButton_toggesToStop() {
        let startButton = app.buttons["Start scanning"]
        XCTAssertTrue(startButton.waitForExistence(timeout: 5))
        startButton.tap()
        XCTAssertTrue(app.buttons["Stop scanning"].waitForExistence(timeout: 3))
    }

    func test_tapStop_togglesBackToStart() {
        app.buttons["Start scanning"].tap()
        app.buttons["Stop scanning"].tap()
        XCTAssertTrue(app.buttons["Start scanning"].waitForExistence(timeout: 3))
    }

    // MARK: - Settings sheet

    func test_settingsButton_presentsSheet() {
        app.buttons["Settings"].tap()
        XCTAssertTrue(app.navigationBars["Settings"].waitForExistence(timeout: 3))
    }

    func test_settingsDone_dismissesSheet() {
        app.buttons["Settings"].tap()
        XCTAssertTrue(app.navigationBars["Settings"].waitForExistence(timeout: 3))
        app.buttons["Done"].tap()
        XCTAssertTrue(app.buttons["Start scanning"].waitForExistence(timeout: 3))
    }

    // MARK: - Settings controls

    func test_settingsContainsConfidenceSlider() {
        app.buttons["Settings"].tap()
        XCTAssertTrue(app.sliders.firstMatch.waitForExistence(timeout: 3))
    }

    func test_settingsContainsAudioToggle() {
        app.buttons["Settings"].tap()
        XCTAssertTrue(app.switches["Audio alert"].waitForExistence(timeout: 3))
    }
}
