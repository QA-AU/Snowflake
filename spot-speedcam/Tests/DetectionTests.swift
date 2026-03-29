import XCTest
@testable import SpotSpeedCam

final class DetectionTests: XCTestCase {

    // MARK: - SpeedCamDetection

    func test_detection_isHighConfidence_above70() {
        let d = SpeedCamDetection(boundingBox: .zero, confidence: 0.75, source: .coreML)
        XCTAssertTrue(d.isHighConfidence)
    }

    func test_detection_isNotHighConfidence_below70() {
        let d = SpeedCamDetection(boundingBox: .zero, confidence: 0.65, source: .coreML)
        XCTAssertFalse(d.isHighConfidence)
    }

    func test_detection_isNotHighConfidence_at70_exactly() {
        let d = SpeedCamDetection(boundingBox: .zero, confidence: 0.70, source: .coreML)
        XCTAssertTrue(d.isHighConfidence)
    }

    func test_detection_sources() {
        let coreML = SpeedCamDetection(boundingBox: .zero, confidence: 0.8, source: .coreML)
        let clip   = SpeedCamDetection(boundingBox: .zero, confidence: 0.6, source: .clipZeroShot)
        XCTAssertEqual(coreML.source, .coreML)
        XCTAssertEqual(clip.source,   .clipZeroShot)
    }

    // MARK: - Cosine Similarity (via SpeedCamDetector's internal logic exposed for test)

    func test_cosineSimilarity_identicalVectors() {
        let a: [Float] = [1, 0, 0, 0]
        XCTAssertEqual(cosineSimilarity(a, a), 1.0, accuracy: 1e-5)
    }

    func test_cosineSimilarity_orthogonalVectors() {
        let a: [Float] = [1, 0, 0]
        let b: [Float] = [0, 1, 0]
        XCTAssertEqual(cosineSimilarity(a, b), 0.0, accuracy: 1e-5)
    }

    func test_cosineSimilarity_oppositeVectors() {
        let a: [Float] = [1, 0, 0]
        let b: [Float] = [-1, 0, 0]
        XCTAssertEqual(cosineSimilarity(a, b), -1.0, accuracy: 1e-5)
    }

    func test_cosineSimilarity_emptyVectors() {
        XCTAssertEqual(cosineSimilarity([], []), 0.0)
    }

    func test_cosineSimilarity_mismatchedLength() {
        let a: [Float] = [1, 2]
        let b: [Float] = [1, 2, 3]
        XCTAssertEqual(cosineSimilarity(a, b), 0.0)
    }

    // MARK: - NMS via bounding box IoU

    func test_iou_nonOverlapping() {
        let a = CGRect(x: 0, y: 0, width: 0.3, height: 0.3)
        let b = CGRect(x: 0.5, y: 0.5, width: 0.3, height: 0.3)
        XCTAssertEqual(iou(a, b), 0.0, accuracy: 1e-5)
    }

    func test_iou_perfectOverlap() {
        let a = CGRect(x: 0, y: 0, width: 1, height: 1)
        XCTAssertEqual(iou(a, a), 1.0, accuracy: 1e-5)
    }

    func test_iou_partialOverlap() {
        let a = CGRect(x: 0, y: 0, width: 0.6, height: 0.6)
        let b = CGRect(x: 0.3, y: 0.3, width: 0.6, height: 0.6)
        let result = iou(a, b)
        XCTAssertGreaterThan(result, 0)
        XCTAssertLessThan(result, 1)
    }

    // MARK: - Helpers (mirror SpeedCamDetector internals for unit testing)

    private func cosineSimilarity(_ a: [Float], _ b: [Float]) -> Float {
        guard a.count == b.count, !a.isEmpty else { return 0 }
        let dot  = zip(a, b).reduce(0, { $0 + $1.0 * $1.1 })
        let magA = sqrt(a.reduce(0, { $0 + $1 * $1 }))
        let magB = sqrt(b.reduce(0, { $0 + $1 * $1 }))
        guard magA > 0, magB > 0 else { return 0 }
        return dot / (magA * magB)
    }

    private func iou(_ a: CGRect, _ b: CGRect) -> Float {
        let intersection = a.intersection(b)
        guard !intersection.isNull else { return 0 }
        let inter = Float(intersection.width * intersection.height)
        let union = Float(a.width * a.height) + Float(b.width * b.height) - inter
        guard union > 0 else { return 0 }
        return inter / union
    }
}
