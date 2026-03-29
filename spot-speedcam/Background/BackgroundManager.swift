import Foundation
import AVFoundation
import BackgroundTasks
import UIKit

// MARK: - BackgroundManager

/// Keeps the inference loop alive when the iPhone screen is locked or
/// the user switches to another app.
///
/// Strategy (iPhone 15+, iOS 17+):
/// ┌─────────────────────────────────────────────────────────────────┐
/// │ 1. AVAudioSession(.playback) — declared in entitlements.        │
/// │    The AlertService already activates this session.             │
/// │    As long as the session is active the OS keeps the process    │
/// │    alive indefinitely, exactly like a music app.                │
/// │                                                                 │
/// │ 2. BGProcessingTask — registered as a long-running background   │
/// │    task. Fires whenever the OS grants time (charging + idle).   │
/// │    Used to restart inference after a crash or OS kill.          │
/// │                                                                 │
/// │ 3. UIApplication.beginBackgroundTask — 30-second safety net     │
/// │    for the transition window when the app first backgrounds.    │
/// └─────────────────────────────────────────────────────────────────┘
///
/// Info.plist requirements (add these manually or via Xcode target):
///   UIBackgroundModes: [audio, processing]
///   BGTaskSchedulerPermittedIdentifiers: [com.spotspeedcam.restart]
final class BackgroundManager: ObservableObject {

    // MARK: - Published state

    @Published private(set) var isBackground = false

    // MARK: - Private state

    private var bgTaskIdentifier: UIBackgroundTaskIdentifier = .invalid
    private let processingTaskID = "com.spotspeedcam.restart"
    private var onRestartInference: (() -> Void)?

    // MARK: - Init

    init() {
        registerBGTask()
    }

    // MARK: - Public callbacks

    /// Injected by the root view: called when BGProcessingTask fires and
    /// inference needs to restart.
    func setRestartHandler(_ handler: @escaping () -> Void) {
        onRestartInference = handler
    }

    // MARK: - App lifecycle hooks

    func appDidBackground() {
        isBackground = true
        beginUIBackgroundTask()
        scheduleBGProcessingTask()
    }

    func appDidForeground() {
        isBackground = false
        endUIBackgroundTask()
    }

    // MARK: - UIBackgroundTask (30-second transition window)

    private func beginUIBackgroundTask() {
        bgTaskIdentifier = UIApplication.shared.beginBackgroundTask(withName: "SpeedCamScan") { [weak self] in
            // Expiry handler — OS is about to suspend us. Clean up gracefully.
            self?.endUIBackgroundTask()
        }
    }

    private func endUIBackgroundTask() {
        guard bgTaskIdentifier != .invalid else { return }
        UIApplication.shared.endBackgroundTask(bgTaskIdentifier)
        bgTaskIdentifier = .invalid
    }

    // MARK: - BGProcessingTask registration

    private func registerBGTask() {
        BGTaskScheduler.shared.register(
            forTaskWithIdentifier: processingTaskID,
            using: nil
        ) { [weak self] task in
            guard let self, let task = task as? BGProcessingTask else { return }
            self.handleBGProcessingTask(task)
        }
    }

    private func scheduleBGProcessingTask() {
        let request = BGProcessingTaskRequest(identifier: processingTaskID)
        request.requiresNetworkConnectivity = false
        request.requiresExternalPower = false
        try? BGTaskScheduler.shared.submit(request)
    }

    private func handleBGProcessingTask(_ task: BGProcessingTask) {
        // Restart inference if the process was killed and relaunched
        onRestartInference?()

        task.expirationHandler = { [weak self] in
            // OS is reclaiming time — schedule again for next opportunity
            self?.scheduleBGProcessingTask()
            task.setTaskCompleted(success: false)
        }

        // Keep running — task completes only when the app foregrounds
        // (this is intentional for a monitoring app)
    }
}
