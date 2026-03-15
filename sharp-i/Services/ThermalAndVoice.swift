import Foundation
import AVFoundation
import UIKit

// MARK: - ThermalStateManager

final class ThermalStateManager: ObservableObject {

    @Published var currentState: ProcessInfo.ThermalState = .nominal
    @Published var isCritical = false
    @Published var isSerious  = false

    private var observer: NSObjectProtocol?

    init() {
        observer = NotificationCenter.default.addObserver(
            forName: ProcessInfo.thermalStateDidChangeNotification,
            object:  nil,
            queue:   .main
        ) { [weak self] _ in self?.update() }
        update()
    }

    deinit {
        if let observer { NotificationCenter.default.removeObserver(observer) }
    }

    private func update() {
        let state = ProcessInfo.processInfo.thermalState
        currentState = state
        isSerious    = state == .serious
        isCritical   = state == .critical
    }
}

// MARK: - VoiceReadoutService

final class VoiceReadoutService: NSObject, AVSpeechSynthesizerDelegate {

    private let synthesizer = AVSpeechSynthesizer()
    private(set) var isSpeaking = false

    override init() {
        super.init()
        synthesizer.delegate = self
    }

    /// Reads the top-3 eligible results aloud.
    func read(results: [DetectionResult], amberEnabled: Bool = false) {
        synthesizer.stopSpeaking(at: .immediate)
        let eligible = results.filter { $0.tier.readoutEnabled || (amberEnabled && $0.tier == .amber) }
        guard !eligible.isEmpty else { return }

        let text = eligible.prefix(3).map { r in
            "\(r.label), \(Int(r.confidence * 100)) percent"
        }.joined(separator: ". ")

        let utterance = AVSpeechUtterance(string: text)
        utterance.rate  = AVSpeechUtteranceDefaultSpeechRate
        utterance.voice = AVSpeechSynthesisVoice(language: "en-US")
        synthesizer.speak(utterance)
    }

    func stop() { synthesizer.stopSpeaking(at: .immediate) }

    func speechSynthesizer(_ synthesizer: AVSpeechSynthesizer, didStart utterance: AVSpeechUtterance) {
        isSpeaking = true
    }

    func speechSynthesizer(_ synthesizer: AVSpeechSynthesizer, didFinish utterance: AVSpeechUtterance) {
        isSpeaking = false
    }
}

// MARK: - LiveInferenceEngine
// Stub — will be replaced with a CoreML-backed continuous-frame inference loop.

final class LiveInferenceEngine: ObservableObject {

    @Published var isRunning = false

    func startLiveInference(frameProvider: AsyncStream<CVPixelBuffer>) {
        // TODO: bind CVPixelBuffer stream to CoreML model
        isRunning = true
    }

    func stop() { isRunning = false }
}
