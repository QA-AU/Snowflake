import AVFoundation
import CoreVideo
import UIKit

/// Wraps AVCaptureSession for continuous rear-camera streaming.
///
/// Key differences from Sharp-I's CameraSession:
/// - Rear camera (not front): we're looking at what's ahead of us, not at ourselves.
/// - Continuous frame delivery via delegate; NOT a one-shot async grab.
/// - Higher frame rate (30 fps) for real-time road scanning.
/// - The inference loop throttles internally to avoid thermal issues.
final class CameraSession: NSObject, ObservableObject {

    // MARK: - Published state

    @Published var isRunning = false
    @Published var permissionDenied = false
    @Published var previewLayer: AVCaptureVideoPreviewLayer?

    // MARK: - Internal AVFoundation objects

    private let captureSession = AVCaptureSession()
    private let videoOutput = AVCaptureVideoDataOutput()
    private let sessionQueue = DispatchQueue(label: "com.spotspeedcam.camera.session", qos: .userInitiated)

    // MARK: - Frame delivery

    /// Called on sessionQueue with each new frame.
    var onFrame: ((CVPixelBuffer) -> Void)?

    // MARK: - Lifecycle

    func requestPermissionAndStart() {
        switch AVCaptureDevice.authorizationStatus(for: .video) {
        case .authorized:
            sessionQueue.async { self.configure() }
        case .notDetermined:
            AVCaptureDevice.requestAccess(for: .video) { [weak self] granted in
                guard let self else { return }
                if granted {
                    self.sessionQueue.async { self.configure() }
                } else {
                    DispatchQueue.main.async { self.permissionDenied = true }
                }
            }
        default:
            DispatchQueue.main.async { self.permissionDenied = true }
        }
    }

    func stop() {
        sessionQueue.async { [weak self] in
            self?.captureSession.stopRunning()
            DispatchQueue.main.async { self?.isRunning = false }
        }
    }

    func resume() {
        sessionQueue.async { [weak self] in
            guard let self, !self.captureSession.isRunning else { return }
            self.captureSession.startRunning()
            DispatchQueue.main.async { self.isRunning = true }
        }
    }

    // MARK: - Private setup

    private func configure() {
        captureSession.beginConfiguration()
        captureSession.sessionPreset = .hd1280x720

        // Rear camera
        guard
            let device = AVCaptureDevice.default(.builtInWideAngleCamera, for: .video, position: .back),
            let input = try? AVCaptureDeviceInput(device: device),
            captureSession.canAddInput(input)
        else {
            captureSession.commitConfiguration()
            return
        }
        captureSession.addInput(input)

        // 30 fps
        configureFrameRate(device: device, fps: 30)

        // Video output — 32-bit BGRA for CoreML compatibility
        videoOutput.videoSettings = [
            kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA
        ]
        videoOutput.alwaysDiscardsLateVideoFrames = true
        videoOutput.setSampleBufferDelegate(self, queue: sessionQueue)

        guard captureSession.canAddOutput(videoOutput) else {
            captureSession.commitConfiguration()
            return
        }
        captureSession.addOutput(videoOutput)

        // Stabilise video connection orientation
        if let connection = videoOutput.connection(with: .video) {
            connection.videoRotationAngle = 90 // portrait
        }

        captureSession.commitConfiguration()

        // Build preview layer on main thread
        let layer = AVCaptureVideoPreviewLayer(session: captureSession)
        layer.videoGravity = .resizeAspectFill
        DispatchQueue.main.async { self.previewLayer = layer }

        captureSession.startRunning()
        DispatchQueue.main.async { self.isRunning = true }
    }

    private func configureFrameRate(device: AVCaptureDevice, fps: Int) {
        guard let format = device.formats.first(where: { format in
            let dims = CMVideoFormatDescriptionGetDimensions(format.formatDescription)
            return dims.width == 1280 && dims.height == 720
        }) else { return }

        let targetDuration = CMTimeMake(value: 1, timescale: Int32(fps))
        do {
            try device.lockForConfiguration()
            device.activeFormat = format
            device.activeVideoMinFrameDuration = targetDuration
            device.activeVideoMaxFrameDuration = targetDuration
            device.unlockForConfiguration()
        } catch {}
    }
}

// MARK: - AVCaptureVideoDataOutputSampleBufferDelegate

extension CameraSession: AVCaptureVideoDataOutputSampleBufferDelegate {
    func captureOutput(
        _ output: AVCaptureOutput,
        didOutput sampleBuffer: CMSampleBuffer,
        from connection: AVCaptureConnection
    ) {
        guard let pixelBuffer = CMSampleBufferGetImageBuffer(sampleBuffer) else { return }
        onFrame?(pixelBuffer)
    }
}
