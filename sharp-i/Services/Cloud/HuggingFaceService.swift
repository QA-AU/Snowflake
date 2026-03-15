import Foundation
import UIKit

// MARK: - HuggingFaceService
// Prototype path: all inference via Hugging Face Inference API.
// Production path: replace with on-device CoreML (CLIP, YOLOv8, FaceNet).

final class HuggingFaceService {

    static let shared = HuggingFaceService()
    private init() {}

    private let session = URLSession.shared

    private var apiKey: String {
        // Re-read on every call so key changes are picked up without restart.
        AppSettings().huggingFaceAPIKey
    }

    private enum Endpoint {
        static let clipText  = "https://api-inference.huggingface.co/models/openai/clip-vit-base-patch32"
        static let clipImage = "https://api-inference.huggingface.co/models/openai/clip-vit-base-patch32"
        static let detr      = "https://api-inference.huggingface.co/models/facebook/detr-resnet-50"
    }

    // MARK: - Text Embedding (CLIP)

    func textEmbedding(for text: String) async throws -> [Float] {
        let data = try await postJSON(endpoint: Endpoint.clipText, body: ["inputs": text])
        return try JSONDecoder().decode([Float].self, from: data)
    }

    // MARK: - Image Embedding (CLIP)

    func imageEmbedding(for image: UIImage) async throws -> [Float] {
        guard let jpeg = image.jpegData(compressionQuality: 0.8) else {
            throw HFError.imageEncodingFailed
        }
        let b64 = jpeg.base64EncodedString()
        let data = try await postJSON(endpoint: Endpoint.clipImage,
                                      body: ["inputs": ["image": b64]])
        return try JSONDecoder().decode([Float].self, from: data)
    }

    // MARK: - Object Detection (DETR)

    func detectObjects(in image: UIImage) async throws -> [RawDetection] {
        guard let jpeg = image.jpegData(compressionQuality: 0.8) else {
            throw HFError.imageEncodingFailed
        }
        var request = URLRequest(url: try validURL(Endpoint.detr))
        request.httpMethod = "POST"
        request.setValue("Bearer \(apiKey)", forHTTPHeaderField: "Authorization")
        request.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
        request.httpBody = jpeg

        let (data, response) = try await session.data(for: request)
        try validate(response: response)
        return try JSONDecoder().decode([RawDetection].self, from: data)
    }

    // MARK: - Cosine Similarity

    func cosineSimilarity(_ a: [Float], _ b: [Float]) -> Float {
        guard a.count == b.count, !a.isEmpty else { return 0 }
        let dot  = zip(a, b).map(*).reduce(0, +)
        let magA = sqrt(a.map { $0 * $0 }.reduce(0, +))
        let magB = sqrt(b.map { $0 * $0 }.reduce(0, +))
        guard magA > 0, magB > 0 else { return 0 }
        return dot / (magA * magB)
    }

    // MARK: - Full Search Pipeline

    func fullSearch(image: UIImage, queryEmbedding: [Float]) async throws -> [DetectionResult] {
        async let detectionsTask  = detectObjects(in: image)
        async let imgEmbeddingTask = imageEmbedding(for: image)

        let (rawDetections, imageEmb) = try await (detectionsTask, imgEmbeddingTask)
        let imageSimilarity = cosineSimilarity(queryEmbedding, imageEmb)

        return rawDetections.map { det in
            let box = BoundingBoxData(
                x:      det.box.xmin,
                y:      det.box.ymin,
                width:  det.box.xmax - det.box.xmin,
                height: det.box.ymax - det.box.ymin
            )
            // Blend DETR object score (60%) with scene-level CLIP similarity (40%).
            let blended = (det.score * 0.6) + (imageSimilarity * 0.4)
            return DetectionResult(label: det.label, confidence: blended, boundingBox: box)
        }
    }

    // MARK: - Helpers

    private func postJSON(endpoint: String, body: Any) async throws -> Data {
        var request = URLRequest(url: try validURL(endpoint))
        request.httpMethod = "POST"
        request.setValue("Bearer \(apiKey)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: body)

        let (data, response) = try await session.data(for: request)
        try validate(response: response)
        return data
    }

    private func validURL(_ string: String) throws -> URL {
        guard let url = URL(string: string) else { throw HFError.invalidURL }
        return url
    }

    private func validate(response: URLResponse) throws {
        guard let http = response as? HTTPURLResponse else { return }
        switch http.statusCode {
        case 200..<300: return
        case 401:       throw HFError.unauthorized
        case 503:       throw HFError.modelLoading
        default:        throw HFError.httpError(http.statusCode)
        }
    }

    // MARK: - Errors

    enum HFError: LocalizedError {
        case invalidURL
        case imageEncodingFailed
        case unauthorized
        case modelLoading
        case httpError(Int)

        var errorDescription: String? {
            switch self {
            case .invalidURL:         return "Invalid Hugging Face endpoint URL."
            case .imageEncodingFailed: return "Failed to encode image as JPEG."
            case .unauthorized:       return "Invalid Hugging Face API key."
            case .modelLoading:       return "Model is loading — please try again shortly."
            case .httpError(let c):   return "HTTP \(c) from Hugging Face."
            }
        }
    }
}

// MARK: - Response Types

struct RawDetection: Decodable {
    let label: String
    let score: Float
    let box: RawBox
}

struct RawBox: Decodable {
    let xmin: Float
    let ymin: Float
    let xmax: Float
    let ymax: Float
}
