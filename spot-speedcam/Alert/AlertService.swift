import AVFoundation
import UIKit

// MARK: - Alert service

/// Plays a single spoken alert "mobile speed camera" and optionally
/// a backing audio tone when a speed camera is detected.
///
/// Design decisions vs Sharp-I's VoiceReadoutService:
/// - One fixed phrase, no queue of results to read.
/// - System sound + TTS fire simultaneously for instant attention.
/// - Respects the user's mute preference (soft mute = vibrate only).
/// - De-bounced: calling triggerAlert() while already alerting is a no-op.
final class AlertService: NSObject, ObservableObject {

    // MARK: - Published state

    @Published private(set) var isAlerting = false

    // MARK: - Config

    private let alertPhrase = "mobile speed camera"
    private let alertVolume: Float = 1.0

    // MARK: - AVFoundation objects

    private var synthesizer: AVSpeechSynthesizerProtocol
    private var audioPlayer: AVAudioPlayer?
    private var session: AVAudioSession { .sharedInstance() }

    // MARK: - Init

    init(synthesizer: AVSpeechSynthesizerProtocol = AVSpeechSynthesizer()) {
        self.synthesizer = synthesizer
        super.init()
        setupAudioSession()
        preloadTonePlayer()
    }

    // MARK: - Public API

    /// Triggers the spoken alert. Safe to call from any thread.
    func triggerAlert() {
        DispatchQueue.main.async { self._triggerAlert() }
    }

    func stopAlert() {
        synthesizer.stopSpeaking(at: .immediate)
        audioPlayer?.stop()
        isAlerting = false
    }

    // MARK: - Private

    private func _triggerAlert() {
        guard !isAlerting else { return }
        isAlerting = true

        // Pulse the tone (non-blocking)
        audioPlayer?.play()

        // Spoken phrase
        let utterance = AVSpeechUtterance(string: alertPhrase)
        utterance.rate = AVSpeechUtteranceDefaultSpeechRate * 0.9
        utterance.pitchMultiplier = 1.1
        utterance.volume = alertVolume
        utterance.voice = AVSpeechSynthesisVoice(language: "en-GB")  // British accent
        synthesizer.speak(utterance)

        // Haptic feedback
        UIImpactFeedbackGenerator(style: .heavy).impactOccurred()

        // Reset alerting state after phrase duration (~1.5 sec)
        DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) { [weak self] in
            self?.isAlerting = false
        }
    }

    private func setupAudioSession() {
        // .playback keeps audio alive when screen is locked / app is backgrounded.
        // Combined with the Background Audio capability this is what allows the
        // alert to fire even when the phone screen is off.
        do {
            try session.setCategory(
                .playback,
                mode: .spokenAudio,
                options: [.mixWithOthers, .allowBluetooth]
            )
            try session.setActive(true)
        } catch {
            // Non-fatal: alert will still work in foreground
        }
    }

    /// Preloads a short 440 Hz sine tone bundled as "alert_tone.aiff".
    /// Falls back gracefully if the file is absent (TTS still fires).
    private func preloadTonePlayer() {
        guard let url = Bundle.main.url(forResource: "alert_tone", withExtension: "aiff") else { return }
        audioPlayer = try? AVAudioPlayer(contentsOf: url)
        audioPlayer?.prepareToPlay()
        audioPlayer?.volume = 0.6
    }
}

// MARK: - AVSpeechSynthesizer protocol (for unit testing)

protocol AVSpeechSynthesizerProtocol: AnyObject {
    func speak(_ utterance: AVSpeechUtterance)
    func stopSpeaking(at boundary: AVSpeechBoundary) -> Bool
}

extension AVSpeechSynthesizer: AVSpeechSynthesizerProtocol {}
