import AVFoundation
import UIKit
import SwiftUI

// MARK: - CameraSession

@MainActor
final class CameraSession: NSObject, ObservableObject {

    @Published var isRunning        = false
    @Published var permissionGranted = false
    @Published var permissionDenied  = false

    let captureSession = AVCaptureSession()
    private var videoOutput: AVCaptureVideoDataOutput?
    private var frameContinuation: CheckedContinuation<CVPixelBuffer, Error>?

    // MARK: - Setup

    func configure() async {
        await requestPermission()
        guard permissionGranted else { return }
        setupSession()
    }

    private func requestPermission() async {
        let status = AVCaptureDevice.authorizationStatus(for: .video)
        switch status {
        case .authorized:
            permissionGranted = true
        case .notDetermined:
            permissionGranted = await AVCaptureDevice.requestAccess(for: .video)
            if !permissionGranted { permissionDenied = true }
        default:
            permissionDenied = true
        }
    }

    private func setupSession() {
        captureSession.beginConfiguration()
        captureSession.sessionPreset = .hd1280x720

        guard
            let device = AVCaptureDevice.default(.builtInWideAngleCamera, for: .video, position: .back),
            let input  = try? AVCaptureDeviceInput(device: device),
            captureSession.canAddInput(input)
        else {
            captureSession.commitConfiguration()
            return
        }
        captureSession.addInput(input)

        let output = AVCaptureVideoDataOutput()
        output.videoSettings = [kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA]
        output.setSampleBufferDelegate(self, queue: DispatchQueue(label: "com.sharpi.camera"))
        if captureSession.canAddOutput(output) {
            captureSession.addOutput(output)
            videoOutput = output
        }
        captureSession.commitConfiguration()
    }

    // MARK: - Control

    func start() {
        guard !captureSession.isRunning else { return }
        Task.detached { [weak self] in
            self?.captureSession.startRunning()
            await MainActor.run { self?.isRunning = true }
        }
    }

    func stop() {
        guard captureSession.isRunning else { return }
        Task.detached { [weak self] in
            self?.captureSession.stopRunning()
            await MainActor.run { self?.isRunning = false }
        }
    }

    // MARK: - Async Frame Grab (tap-to-scan)

    func grabFrame() async throws -> CVPixelBuffer {
        try await withCheckedThrowingContinuation { continuation in
            frameContinuation = continuation
        }
    }
}

// MARK: - AVCaptureVideoDataOutputSampleBufferDelegate

extension CameraSession: AVCaptureVideoDataOutputSampleBufferDelegate {
    nonisolated func captureOutput(
        _ output: AVCaptureOutput,
        didOutput sampleBuffer: CMSampleBuffer,
        from connection: AVCaptureConnection
    ) {
        guard let pixelBuffer = CMSampleBufferGetImageBuffer(sampleBuffer) else { return }
        Task { @MainActor in
            if let continuation = frameContinuation {
                frameContinuation = nil
                continuation.resume(returning: pixelBuffer)
            }
        }
    }
}

// MARK: - CameraPreviewView

struct CameraPreviewView: UIViewRepresentable {
    let session: AVCaptureSession

    func makeUIView(context: Context) -> PreviewUIView {
        let view = PreviewUIView()
        view.previewLayer.session      = session
        view.previewLayer.videoGravity = .resizeAspectFill
        return view
    }

    func updateUIView(_ uiView: PreviewUIView, context: Context) {}

    class PreviewUIView: UIView {
        override class var layerClass: AnyClass { AVCaptureVideoPreviewLayer.self }
        var previewLayer: AVCaptureVideoPreviewLayer { layer as! AVCaptureVideoPreviewLayer }
    }
}
