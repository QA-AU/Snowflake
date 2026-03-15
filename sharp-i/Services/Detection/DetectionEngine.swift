import Foundation
import UIKit

// MARK: - DetectionEngine

@MainActor
final class DetectionEngine: ObservableObject {

    // MARK: - Dependencies

    private let huggingFaceService: HuggingFaceService
    private var embeddingCache: [String: [Float]] = [:]

    // MARK: - State

    @Published var isRunning = false
    @Published var lastResults: [DetectionResult] = []

    // MARK: - Init

    init(huggingFaceService: HuggingFaceService = .shared) {
        self.huggingFaceService = huggingFaceService
    }

    // MARK: - Search

    func search(query: SearchQuery, in image: UIImage) async throws -> [DetectionResult] {
        isRunning = true
        defer { isRunning = false }

        let queryEmbedding = try await cachedTextEmbedding(for: query.text)
        let raw = try await huggingFaceService.fullSearch(image: image, queryEmbedding: queryEmbedding)

        let results = assignLabels(
            results: sort(results: filter(results: raw)),
            query: query.text
        )
        lastResults = results
        return results
    }

    // MARK: - Embedding Cache

    private func cachedTextEmbedding(for text: String) async throws -> [Float] {
        if let cached = embeddingCache[text] { return cached }
        let embedding = try await huggingFaceService.textEmbedding(for: text)
        embeddingCache[text] = embedding
        return embedding
    }

    func clearCache() {
        embeddingCache.removeAll()
    }

    // MARK: - Filter / Sort / Label

    private func filter(results: [DetectionResult]) -> [DetectionResult] {
        results.filter { $0.confidence >= 0.50 }
    }

    private func sort(results: [DetectionResult]) -> [DetectionResult] {
        results.sorted { $0.confidence > $1.confidence }
    }

    private func assignLabels(results: [DetectionResult], query: String) -> [DetectionResult] {
        // Labels arrive from DETR; return as-is.
        // Future: append user query context to ambiguous labels.
        results
    }
}
