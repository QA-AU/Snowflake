import XCTest
@testable import SharpI

final class DetectionTests: XCTestCase {

    // MARK: - Confidence Tier Assignment

    func testTierGreen() {
        let r = DetectionResult(label: "backpack", confidence: 0.90, boundingBox: .zero)
        XCTAssertEqual(r.tier, .green)
    }

    func testTierYellow() {
        let r = DetectionResult(label: "backpack", confidence: 0.72, boundingBox: .zero)
        XCTAssertEqual(r.tier, .yellow)
    }

    func testTierAmber() {
        let r = DetectionResult(label: "backpack", confidence: 0.55, boundingBox: .zero)
        XCTAssertEqual(r.tier, .amber)
    }

    func testTierBoundaryGreenYellow() {
        // Exactly 0.85 → yellow (> 0.85 required for green)
        let r = DetectionResult(label: "bag", confidence: 0.85, boundingBox: .zero)
        XCTAssertEqual(r.tier, .yellow)
    }

    func testTierBoundaryYellowAmber() {
        // Exactly 0.60 → yellow
        let r = DetectionResult(label: "bag", confidence: 0.60, boundingBox: .zero)
        XCTAssertEqual(r.tier, .yellow)
    }

    // MARK: - Cosine Similarity

    func testCosineSimilarityIdentical() {
        let v: [Float] = [1, 0, 0]
        XCTAssertEqual(HuggingFaceService.shared.cosineSimilarity(v, v), 1.0, accuracy: 0.001)
    }

    func testCosineSimilarityOrthogonal() {
        let a: [Float] = [1, 0, 0]
        let b: [Float] = [0, 1, 0]
        XCTAssertEqual(HuggingFaceService.shared.cosineSimilarity(a, b), 0.0, accuracy: 0.001)
    }

    func testCosineSimilarityOpposite() {
        let a: [Float] = [1, 0]
        let b: [Float] = [-1, 0]
        XCTAssertEqual(HuggingFaceService.shared.cosineSimilarity(a, b), -1.0, accuracy: 0.001)
    }

    func testCosineSimilarityEmpty() {
        XCTAssertEqual(HuggingFaceService.shared.cosineSimilarity([], []), 0.0)
    }

    func testCosineSimilarityMismatchedLength() {
        let a: [Float] = [1, 0]
        let b: [Float] = [1, 0, 0]
        XCTAssertEqual(HuggingFaceService.shared.cosineSimilarity(a, b), 0.0)
    }

    // MARK: - BoundingBoxData

    func testBoundingBoxCGRect() {
        let box = BoundingBoxData(x: 0.1, y: 0.2, width: 0.3, height: 0.4)
        let rect = box.cgRect
        XCTAssertEqual(rect.origin.x, 0.1, accuracy: 0.001)
        XCTAssertEqual(rect.origin.y, 0.2, accuracy: 0.001)
        XCTAssertEqual(rect.width,    0.3, accuracy: 0.001)
        XCTAssertEqual(rect.height,   0.4, accuracy: 0.001)
    }

    // MARK: - DetectionEngine

    @MainActor
    func testClearCacheDoesNotCrash() {
        let engine = DetectionEngine()
        engine.clearCache()
        engine.clearCache()  // second call is safe
    }

    @MainActor
    func testInitialStateIsIdle() {
        let engine = DetectionEngine()
        XCTAssertFalse(engine.isRunning)
        XCTAssertTrue(engine.lastResults.isEmpty)
    }
}

private extension BoundingBoxData {
    static var zero: BoundingBoxData { .init(x: 0, y: 0, width: 0, height: 0) }
}
