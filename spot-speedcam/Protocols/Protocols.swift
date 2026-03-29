import AVFoundation
import CoreVideo
import UIKit

// MARK: - InferenceEngine

/// Abstraction over the detection back-end. Allows unit tests to inject
/// a deterministic stub without spinning up real camera or CoreML hardware.
protocol InferenceEngineProtocol: AnyObject {
    func detect(pixelBuffer: CVPixelBuffer) async throws -> [SpeedCamDetection]
}

// MARK: - AlertEmitter

/// Abstraction over AlertService so tests can assert that alerts fired
/// without actually triggering audio/haptics.
protocol AlertEmitterProtocol: AnyObject {
    func triggerAlert()
    func stopAlert()
    var isAlerting: Bool { get }
}

extension AlertService: AlertEmitterProtocol {}

// MARK: - FrameSource

/// Abstraction over CameraSession for tests that supply synthetic frames.
protocol FrameSourceProtocol: AnyObject {
    var onFrame: ((CVPixelBuffer) -> Void)? { get set }
    func requestPermissionAndStart()
    func stop()
    func resume()
}

extension CameraSession: FrameSourceProtocol {}

// MARK: - ThermalStateProvider

/// Abstraction over ThermalManager for deterministic thermal unit tests.
protocol ThermalStateProviderProtocol: AnyObject {
    var isCritical: Bool { get }
    var isSerious: Bool { get }
}

extension ThermalManager: ThermalStateProviderProtocol {}
