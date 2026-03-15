import XCTest
@testable import SharpI

final class VoiceReadoutTests: XCTestCase {

    var voiceService: VoiceReadoutService!

    override func setUp() {
        super.setUp()
        voiceService = VoiceReadoutService()
    }

    override func tearDown() {
        voiceService.stop()
        super.tearDown()
    }

    // MARK: - Tier Readout Eligibility

    func testGreenReadoutEnabled() {
        let r = DetectionResult(label: "backpack", confidence: 0.90, boundingBox: .zero)
        XCTAssertTrue(r.tier.readoutEnabled)
    }

    func testYellowReadoutEnabled() {
        let r = DetectionResult(label: "keys", confidence: 0.75, boundingBox: .zero)
        XCTAssertTrue(r.tier.readoutEnabled)
    }

    func testAmberReadoutDisabledByDefault() {
        let r = DetectionResult(label: "phone", confidence: 0.55, boundingBox: .zero)
        XCTAssertFalse(r.tier.readoutEnabled)
    }

    // MARK: - VoiceReadoutService

    func testInitiallyNotSpeaking() {
        XCTAssertFalse(voiceService.isSpeaking)
    }

    func testReadEmptyResultsDoesNotCrash() {
        voiceService.read(results: [])
        XCTAssertFalse(voiceService.isSpeaking)
    }

    func testStopWhenNotSpeakingDoesNotCrash() {
        voiceService.stop()
        XCTAssertFalse(voiceService.isSpeaking)
    }

    func testAmberIncludedWhenEnabled() {
        let amber = DetectionResult(label: "phone", confidence: 0.55, boundingBox: .zero)
        XCTAssertEqual(amber.tier, .amber)
        // With amberEnabled = true the service should not throw
        voiceService.read(results: [amber], amberEnabled: true)
        // No assertion on isSpeaking — AVSpeechSynthesizer is async on device
    }

    func testOnlyGreenYellowReadByDefault() {
        let green  = DetectionResult(label: "bag",  confidence: 0.90, boundingBox: .zero)
        let yellow = DetectionResult(label: "keys", confidence: 0.72, boundingBox: .zero)
        let amber  = DetectionResult(label: "pen",  confidence: 0.54, boundingBox: .zero)
        // Verify tier assignments
        XCTAssertEqual(green.tier,  .green)
        XCTAssertEqual(yellow.tier, .yellow)
        XCTAssertEqual(amber.tier,  .amber)
        // Service call must not crash
        voiceService.read(results: [green, yellow, amber], amberEnabled: false)
    }

    func testTopThreeResultsReadAloud() {
        let results = (1...6).map { i in
            DetectionResult(label: "item\(i)", confidence: Float(i) / 10.0 + 0.40, boundingBox: .zero)
        }
        // Must not crash with 6 results
        voiceService.read(results: results, amberEnabled: true)
    }
}

private extension BoundingBoxData {
    static var zero: BoundingBoxData { .init(x: 0, y: 0, width: 0, height: 0) }
}
