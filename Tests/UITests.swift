import XCTest

final class UITests: XCTestCase {

    var app: XCUIApplication!

    override func setUpWithError() throws {
        continueAfterFailure = false
        app = XCUIApplication()
        app.launch()
    }

    // MARK: - Launch

    func testAppLaunches() {
        XCTAssertEqual(app.state, .runningForeground)
    }

    // MARK: - Search Screen

    func testSearchScreenNavTitleVisible() {
        let title = app.navigationBars["Sharp-I"]
        XCTAssertTrue(title.waitForExistence(timeout: 3))
    }

    func testModeToggleExists() {
        let segmented = app.segmentedControls.firstMatch
        XCTAssertTrue(segmented.waitForExistence(timeout: 3))
    }

    func testObjectModeQueryFieldExists() {
        let field = app.textFields.firstMatch
        XCTAssertTrue(field.waitForExistence(timeout: 3))
    }

    func testScanButtonDisabledWithEmptyQuery() {
        let btn = app.buttons["Scan"]
        XCTAssertTrue(btn.waitForExistence(timeout: 3))
        XCTAssertFalse(btn.isEnabled)
    }

    func testScanButtonEnabledAfterTyping() {
        let field = app.textFields.firstMatch
        field.tap()
        field.typeText("red backpack")
        let btn = app.buttons["Scan"]
        XCTAssertTrue(btn.isEnabled)
    }

    // MARK: - Mode Switch

    func testSwitchToPersonMode() {
        let segmented = app.segmentedControls.firstMatch
        XCTAssertTrue(segmented.waitForExistence(timeout: 3))
        segmented.buttons["Person"].tap()
        // Person mode shows at least one text field (name)
        XCTAssertTrue(app.textFields.firstMatch.waitForExistence(timeout: 2))
    }

    // MARK: - Date Chips

    func testTodayChipExists() {
        let chip = app.buttons["Today"]
        XCTAssertTrue(chip.waitForExistence(timeout: 3))
    }

    func testDateChipToggles() {
        let chip = app.buttons["Today"]
        XCTAssertTrue(chip.waitForExistence(timeout: 3))
        chip.tap()
        chip.tap()  // second tap deselects
    }

    // MARK: - Settings Sheet

    func testSettingsButtonOpensSheet() {
        let gear = app.navigationBars.buttons.element(boundBy: 0)
        XCTAssertTrue(gear.waitForExistence(timeout: 3))
        gear.tap()
        XCTAssertTrue(app.staticTexts["Settings"].waitForExistence(timeout: 2))
    }
}
