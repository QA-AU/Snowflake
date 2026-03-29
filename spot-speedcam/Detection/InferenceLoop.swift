import Foundation
import CoreVideo
import Combine

// MARK: - Inference loop state

enum ScanState: Equatable {
    case idle           // Not scanning (stopped by user or thermal throttle)
    case scanning       // Actively processing frames
    case detected       // Speed camera found — cooldown before re-alerting
    case throttled      // Paused due to thermal pressure
    case error(String)  // Detection pipeline failure

    static func == (lhs: ScanState, rhs: ScanState) -> Bool {
        switch (lhs, rhs) {
        case (.idle, .idle), (.scanning, .scanning), (.detected, .detected), (.throttled, .throttled): return true
        case (.error(let a), .error(let b)): return a == b
        default: return false
        }
    }
}

// MARK: - InferenceLoop

/// Drives continuous real-time inference on camera frames.
///
/// Design goals:
/// 1. Process every Nth frame (not every frame) to balance accuracy and battery.
/// 2. After a detection, enter a cooldown to avoid re-alerting on the same vehicle.
/// 3. Surface scanState for the UI HUD.
/// 4. Honour thermal manager signals — pause when device is hot.
///
/// Threading: frames arrive on CameraSession.sessionQueue (serial).
/// Detection is async, started on a detached Task so the camera queue is never blocked.
@MainActor
final class InferenceLoop: ObservableObject {

    // MARK: - Published state

    @Published private(set) var scanState: ScanState = .idle
    @Published private(set) var lastDetection: SpeedCamDetection?
    @Published private(set) var framesProcessed: Int = 0

    // MARK: - Dependencies

    private let detector: SpeedCamDetectorProtocol
    private let alertService: AlertService
    private let thermalManager: ThermalManager
    private let settings: AppSettings

    // MARK: - Config

    /// Only run inference on every Nth frame (30 fps ÷ 5 = 6 inferences/sec)
    private let frameSkip: Int = 5
    /// Seconds to wait before alerting again after a detection
    private let cooldownSeconds: Double = 8.0

    // MARK: - Internal state

    private var frameCounter = 0
    private var isActive = false
    private var cooldownTask: Task<Void, Never>?
    private var thermalCancellable: AnyCancellable?

    // MARK: - Init

    init(
        detector: SpeedCamDetectorProtocol,
        alertService: AlertService,
        thermalManager: ThermalManager,
        settings: AppSettings
    ) {
        self.detector = detector
        self.alertService = alertService
        self.thermalManager = thermalManager
        self.settings = settings

        // React to thermal state changes
        thermalCancellable = thermalManager.$isCritical
            .receive(on: RunLoop.main)
            .sink { [weak self] critical in
                guard let self else { return }
                if critical && self.isActive {
                    self.scanState = .throttled
                } else if !critical && self.scanState == .throttled {
                    self.scanState = .scanning
                }
            }
    }

    // MARK: - Control

    func start() {
        guard !isActive else { return }
        isActive = true
        scanState = .scanning
    }

    func stop() {
        isActive = false
        cooldownTask?.cancel()
        scanState = .idle
    }

    // MARK: - Frame ingestion (called from CameraSession.onFrame)

    /// Called on the camera's serial sessionQueue — must be lightweight.
    /// Heavy work is dispatched off that queue via a detached Task.
    nonisolated func processFrame(_ pixelBuffer: CVPixelBuffer) {
        // Capture loop counter without crossing actor boundary mid-frame
        Task { @MainActor in
            await self.evaluateFrame(pixelBuffer)
        }
    }

    // MARK: - Private inference

    private func evaluateFrame(_ pixelBuffer: CVPixelBuffer) async {
        guard isActive else { return }
        guard scanState == .scanning else { return }      // Skip if throttled/cooldown
        guard thermalManager.isSerious == false else { return }

        frameCounter += 1
        guard frameCounter % frameSkip == 0 else { return } // Frame-skip

        framesProcessed += 1

        do {
            let results = try await detector.detect(pixelBuffer: pixelBuffer)
            handle(results: results)
        } catch {
            scanState = .error(error.localizedDescription)
            // Auto-recover after 3 seconds
            try? await Task.sleep(nanoseconds: 3_000_000_000)
            if isActive { scanState = .scanning }
        }
    }

    private func handle(results: [SpeedCamDetection]) {
        guard let best = results.max(by: { $0.confidence < $1.confidence }) else { return }

        lastDetection = best
        scanState = .detected

        // Fire the alert
        alertService.triggerAlert()

        // Enter cooldown — suppresses re-alerting for the same vehicle
        cooldownTask?.cancel()
        cooldownTask = Task {
            try? await Task.sleep(nanoseconds: UInt64(cooldownSeconds * 1_000_000_000))
            guard !Task.isCancelled, self.isActive else { return }
            await MainActor.run { self.scanState = .scanning }
        }
    }
}
