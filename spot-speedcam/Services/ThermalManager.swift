import Foundation
import Combine

// MARK: - ThermalManager

/// Monitors device thermal state and publishes flags used by InferenceLoop
/// and the UI to throttle or pause inference.
///
/// Reused from Sharp-I with one addition: `isSerious` now also pauses frame
/// processing (not just disables the scan button) since SpeedCam runs
/// continuously rather than on-demand.
final class ThermalManager: ObservableObject {

    @Published private(set) var isCritical = false   // Stop all inference immediately
    @Published private(set) var isSerious  = false   // Slow down / warn user

    private var cancellable: AnyCancellable?

    init() {
        updateState(ProcessInfo.processInfo.thermalState)
        cancellable = NotificationCenter.default
            .publisher(for: ProcessInfo.thermalStateDidChangeNotification)
            .receive(on: RunLoop.main)
            .sink { [weak self] _ in
                self?.updateState(ProcessInfo.processInfo.thermalState)
            }
    }

    private func updateState(_ state: ProcessInfo.ThermalState) {
        isCritical = (state == .critical)
        isSerious  = (state == .serious || state == .critical)
    }

    // MARK: - Human-readable label for status bar

    var statusLabel: String {
        switch ProcessInfo.processInfo.thermalState {
        case .nominal:  return ""
        case .fair:     return ""
        case .serious:  return "Warm — reduced scan rate"
        case .critical: return "Too hot — scanning paused"
        @unknown default: return ""
        }
    }

    var statusColor: String {
        switch ProcessInfo.processInfo.thermalState {
        case .serious:  return "FF6D00"   // Amber
        case .critical: return "D50000"   // Red
        default:        return "00C853"   // Green
        }
    }
}
