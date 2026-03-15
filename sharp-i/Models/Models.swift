import Foundation
import SwiftData

// MARK: - SearchQuery

@Model
final class SearchQuery {
    var id: UUID
    var text: String
    var mode: SearchMode
    var createdAt: Date
    var session: SearchSession?

    init(text: String, mode: SearchMode) {
        self.id = UUID()
        self.text = text
        self.mode = mode
        self.createdAt = Date()
    }

    enum SearchMode: String, Codable {
        case object
        case person
    }
}

// MARK: - DetectionResult

@Model
final class DetectionResult {
    var id: UUID
    var label: String
    var confidence: Float
    var boundingBox: BoundingBoxData
    var tier: ConfidenceTier
    var session: SearchSession?
    var createdAt: Date

    init(label: String, confidence: Float, boundingBox: BoundingBoxData) {
        self.id = UUID()
        self.label = label
        self.confidence = confidence
        self.boundingBox = boundingBox
        self.tier = ConfidenceTier(confidence: confidence)
        self.createdAt = Date()
    }
}

// MARK: - BoundingBoxData

struct BoundingBoxData: Codable {
    var x: Float
    var y: Float
    var width: Float
    var height: Float

    var cgRect: CGRect {
        CGRect(x: CGFloat(x), y: CGFloat(y), width: CGFloat(width), height: CGFloat(height))
    }
}

// MARK: - ConfidenceTier

enum ConfidenceTier: String, Codable {
    case green   // > 0.85
    case yellow  // 0.60 – 0.85
    case amber   // 0.50 – 0.60

    init(confidence: Float) {
        if confidence > 0.85 {
            self = .green
        } else if confidence >= 0.60 {
            self = .yellow
        } else {
            self = .amber
        }
    }

    var color: String {
        switch self {
        case .green:  return "#00C853"
        case .yellow: return "#FFD600"
        case .amber:  return "#FF6D00"
        }
    }

    /// Voice readout on by default for green + yellow; amber off unless user opts in.
    var readoutEnabled: Bool {
        switch self {
        case .green, .yellow: return true
        case .amber:          return false
        }
    }
}

// MARK: - KnownItem

@Model
final class KnownItem {
    var id: UUID
    var name: String
    var embeddingData: Data?
    var lastSeenAt: Date?
    var createdAt: Date

    init(name: String) {
        self.id = UUID()
        self.name = name
        self.createdAt = Date()
    }
}

// MARK: - KnownPerson

@Model
final class KnownPerson {
    var id: UUID
    var name: String
    var faceEmbeddingData: Data?
    var photoLocalIdentifiers: [String]
    var createdAt: Date

    init(name: String) {
        self.id = UUID()
        self.name = name
        self.photoLocalIdentifiers = []
        self.createdAt = Date()
    }
}

// MARK: - SearchSession

@Model
final class SearchSession {
    var id: UUID
    var startedAt: Date
    var endedAt: Date?
    var queries: [SearchQuery]
    var results: [DetectionResult]

    init() {
        self.id = UUID()
        self.startedAt = Date()
        self.queries = []
        self.results = []
    }
}
