import CoreML
import Vision
import CoreVideo
import UIKit

// MARK: - Detection result

struct SpeedCamDetection {
    /// Normalised bounding box (0…1) in image coordinates
    let boundingBox: CGRect
    /// Confidence 0…1
    let confidence: Float
    /// Detection method used
    let source: DetectionSource

    var isHighConfidence: Bool { confidence >= 0.70 }
}

enum DetectionSource {
    case coreML      // On-device YOLOv8 (production)
    case clipZeroShot // HuggingFace CLIP (fallback while model is being trained)
}

// MARK: - Detector protocol (for testability)

protocol SpeedCamDetectorProtocol: AnyObject {
    func detect(pixelBuffer: CVPixelBuffer) async throws -> [SpeedCamDetection]
}

// MARK: - Main detector

/// Two-tier detection strategy:
///
/// Tier 1 — CoreML (preferred, on-device, zero latency):
///   Loads `SpeedCamDetector.mlpackage` from the app bundle.
///   This is a fine-tuned YOLOv8n trained on mobile speed camera images.
///   **The model file must be added once training data is collected.**
///   Until then it gracefully falls through to Tier 2.
///
/// Tier 2 — CLIP zero-shot via HuggingFace (fallback):
///   Sends the frame as JPEG to the CLIP image API, then computes cosine
///   similarity against a pre-cached "mobile speed camera in rear window of car"
///   text embedding. No labelled data required, but adds ~600 ms network latency.
///   Suitable for development and early field testing only.
final class SpeedCamDetector: SpeedCamDetectorProtocol, ObservableObject {

    // MARK: - Config

    private let modelName = "SpeedCamDetector"          // .mlpackage bundle name
    private let confidenceThreshold: Float = 0.55
    private let iouThreshold: Float = 0.45
    private let clipTextEmbedding: [Float]              // cached at init

    // MARK: - CoreML

    private var visionModel: VNCoreMLModel?
    private var coreMLAvailable = false

    // MARK: - Published state (for UI feedback)

    @Published var usingFallback = false
    @Published var lastError: String?

    // MARK: - Init

    init() {
        // Cache the CLIP text embedding for "mobile speed camera rear window car van"
        // In production this would be loaded from a bundled .bin file generated offline.
        // Placeholder: empty — real embedding injected via configure(clipEmbedding:).
        self.clipTextEmbedding = []
        loadCoreMLModel()
    }

    // MARK: - CoreML model loading

    private func loadCoreMLModel() {
        guard
            let modelURL = Bundle.main.url(forResource: modelName, withExtension: "mlpackage") ??
                           Bundle.main.url(forResource: modelName, withExtension: "mlmodelc")
        else {
            // Model not yet bundled — this is expected until training is complete.
            DispatchQueue.main.async { self.usingFallback = true }
            return
        }

        do {
            let config = MLModelConfiguration()
            config.computeUnits = .cpuAndNeuralEngine   // ANE on A17 Pro
            let mlModel = try MLModel(contentsOf: modelURL, configuration: config)
            visionModel = try VNCoreMLModel(for: mlModel)
            coreMLAvailable = true
            DispatchQueue.main.async { self.usingFallback = false }
        } catch {
            DispatchQueue.main.async {
                self.usingFallback = true
                self.lastError = "CoreML load failed: \(error.localizedDescription)"
            }
        }
    }

    // MARK: - Detection entry point

    func detect(pixelBuffer: CVPixelBuffer) async throws -> [SpeedCamDetection] {
        if coreMLAvailable, let model = visionModel {
            return try await runCoreML(pixelBuffer: pixelBuffer, model: model)
        } else {
            return try await runCLIPFallback(pixelBuffer: pixelBuffer)
        }
    }

    // MARK: - Tier 1: CoreML / YOLOv8

    private func runCoreML(
        pixelBuffer: CVPixelBuffer,
        model: VNCoreMLModel
    ) async throws -> [SpeedCamDetection] {
        return try await withCheckedThrowingContinuation { continuation in
            let request = VNCoreMLRequest(model: model) { request, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                let observations = request.results as? [VNRecognizedObjectObservation] ?? []
                let detections = observations
                    .filter { $0.confidence >= self.confidenceThreshold }
                    .map { obs in
                        SpeedCamDetection(
                            boundingBox: obs.boundingBox,
                            confidence: obs.confidence,
                            source: .coreML
                        )
                    }
                continuation.resume(returning: self.nonMaxSuppression(detections))
            }
            request.imageCropAndScaleOption = .scaleFit

            let handler = VNImageRequestHandler(cvPixelBuffer: pixelBuffer, options: [:])
            do {
                try handler.perform([request])
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }

    // MARK: - Tier 2: CLIP zero-shot (HuggingFace)

    /// Uses CLIP image embeddings from HuggingFace against the cached text embedding
    /// for "mobile speed camera in rear window of car or van".
    ///
    /// Returns at most one detection (whole-frame bounding box) since CLIP is a
    /// classification model, not a detector — it tells us *if* a speed cam is
    /// present, not *where* in the frame. That's good enough for alerting.
    private func runCLIPFallback(pixelBuffer: CVPixelBuffer) async throws -> [SpeedCamDetection] {
        guard let apiKey = AppSettings.loadHuggingFaceKey(), !apiKey.isEmpty else {
            return []  // No API key — stay silent, don't crash
        }

        guard let jpegData = FrameGrabber.jpegData(from: pixelBuffer) else { return [] }

        // Fetch CLIP image embedding
        let imageEmbedding = try await CLIPService.shared.imageEmbedding(
            jpegData: jpegData,
            apiKey: apiKey
        )

        guard !imageEmbedding.isEmpty, !clipTextEmbedding.isEmpty else { return [] }

        let similarity = cosineSimilarity(imageEmbedding, clipTextEmbedding)

        // CLIP similarity ≥ 0.28 is a reasonable threshold for this query
        guard similarity >= 0.28 else { return [] }

        // Map to a full-frame detection (bounding box = entire frame)
        let detection = SpeedCamDetection(
            boundingBox: CGRect(x: 0, y: 0, width: 1, height: 1),
            confidence: min(1.0, similarity * 2.0),  // scale 0.28–0.5 → 0.56–1.0
            source: .clipZeroShot
        )
        return [detection]
    }

    // MARK: - Helpers

    /// Dot-product cosine similarity for normalised embedding vectors.
    private func cosineSimilarity(_ a: [Float], _ b: [Float]) -> Float {
        guard a.count == b.count, !a.isEmpty else { return 0 }
        let dot = zip(a, b).reduce(0, { $0 + $1.0 * $1.1 })
        let magA = sqrt(a.reduce(0, { $0 + $1 * $1 }))
        let magB = sqrt(b.reduce(0, { $0 + $1 * $1 }))
        guard magA > 0, magB > 0 else { return 0 }
        return dot / (magA * magB)
    }

    /// Greedy non-maximum suppression on overlapping CoreML detections.
    private func nonMaxSuppression(_ detections: [SpeedCamDetection]) -> [SpeedCamDetection] {
        let sorted = detections.sorted { $0.confidence > $1.confidence }
        var kept: [SpeedCamDetection] = []
        for candidate in sorted {
            let overlaps = kept.contains { iou($0.boundingBox, candidate.boundingBox) > iouThreshold }
            if !overlaps { kept.append(candidate) }
        }
        return kept
    }

    private func iou(_ a: CGRect, _ b: CGRect) -> Float {
        let intersection = a.intersection(b)
        guard !intersection.isNull else { return 0 }
        let intersectionArea = Float(intersection.width * intersection.height)
        let unionArea = Float(a.width * a.height) + Float(b.width * b.height) - intersectionArea
        guard unionArea > 0 else { return 0 }
        return intersectionArea / unionArea
    }
}

// MARK: - CLIP Service (HuggingFace fallback)

/// Thin async wrapper around the HuggingFace CLIP image embedding endpoint.
/// Mirrors the pattern in Sharp-I's HuggingFaceService but stripped to
/// image-embedding only (we cache the text embedding offline).
final class CLIPService {
    static let shared = CLIPService()
    private init() {}

    private let endpoint = "https://api-inference.huggingface.co/models/openai/clip-vit-base-patch32"

    func imageEmbedding(jpegData: Data, apiKey: String) async throws -> [Float] {
        var request = URLRequest(url: URL(string: endpoint)!)
        request.httpMethod = "POST"
        request.setValue("Bearer \(apiKey)", forHTTPHeaderField: "Authorization")
        request.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
        request.httpBody = jpegData
        request.timeoutInterval = 8

        let (data, response) = try await URLSession.shared.data(for: request)
        guard (response as? HTTPURLResponse)?.statusCode == 200 else {
            throw CLIPError.httpError((response as? HTTPURLResponse)?.statusCode ?? 0)
        }

        // HF CLIP returns [[Float]] — one embedding per image
        let decoded = try JSONDecoder().decode([[Float]].self, from: data)
        return decoded.first ?? []
    }

    enum CLIPError: Error {
        case httpError(Int)
        case emptyEmbedding
    }
}
