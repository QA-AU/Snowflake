import XCTest
@testable import SpotSpeedCam
import AVFoundation

// MARK: - Stub synthesizer

final class StubSynthesizer: AVSpeechSynthesizerProtocol {
    private(set) var spokenUtterances: [String] = []
    private(set) var stopCalled = false

    func speak(_ utterance: AVSpeechUtterance) {
        spokenUtterances.append(utterance.speechString)
    }

    @discardableResult
    func stopSpeaking(at boundary: AVSpeechBoundary) -> Bool {
        stopCalled = true
        return true
    }
}

// MARK: - AlertTests

final class AlertTests: XCTestCase {

    func test_alertPhrase_isMobileSpeedCamera() {
        let synth = StubSynthesizer()
        let service = AlertService(synthesizer: synth)

        service.triggerAlert()

        // Allow DispatchQueue.main.async to flush
        let exp = expectation(description: "phrase spoken")
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) { exp.fulfill() }
        wait(for: [exp], timeout: 1.0)

        XCTAssertEqual(synth.spokenUtterances.first, "mobile speed camera")
    }

    func test_debounce_doesNotRepeatWhileAlerting() {
        let synth = StubSynthesizer()
        let service = AlertService(synthesizer: synth)

        service.triggerAlert()
        service.triggerAlert()   // Should be swallowed

        let exp = expectation(description: "flush")
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) { exp.fulfill() }
        wait(for: [exp], timeout: 1.0)

        XCTAssertEqual(synth.spokenUtterances.count, 1)
    }

    func test_stopAlert_callsSynth() {
        let synth = StubSynthesizer()
        let service = AlertService(synthesizer: synth)

        service.triggerAlert()
        service.stopAlert()

        let exp = expectation(description: "flush")
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) { exp.fulfill() }
        wait(for: [exp], timeout: 1.0)

        XCTAssertTrue(synth.stopCalled)
    }

    func test_initialAlertingState_isFalse() {
        let service = AlertService(synthesizer: StubSynthesizer())
        XCTAssertFalse(service.isAlerting)
    }

    func test_alertingState_trueWhileActive() {
        let synth = StubSynthesizer()
        let service = AlertService(synthesizer: synth)

        service.triggerAlert()

        let exp = expectation(description: "flush")
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) { exp.fulfill() }
        wait(for: [exp], timeout: 1.0)

        XCTAssertTrue(service.isAlerting)
    }
}
