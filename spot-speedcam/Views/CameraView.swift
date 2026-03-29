import SwiftUI
import AVFoundation

// MARK: - CameraView

/// Main screen of Spot-SpeedCam. Fullscreen live viewfinder with a
/// minimal heads-up display. The inference loop runs continuously — there
/// is no "scan" button like in Sharp-I.
///
/// Layout layers (bottom → top):
///   1. AVCaptureVideoPreviewLayer (fullscreen, fills safe area)
///   2. StatusOverlay (top: mode badge + thermal warning)
///   3. DetectionFlash (animated ring on detection)
///   4. BottomBar (start/stop toggle + settings gear)
struct CameraView: View {

    // MARK: - Dependencies

    @StateObject private var cameraSession = CameraSession()
    @StateObject private var detector      = SpeedCamDetector()
    @EnvironmentObject private var alertService:    AlertService
    @EnvironmentObject private var thermalManager:  ThermalManager
    @EnvironmentObject private var settings:        AppSettings
    @EnvironmentObject private var backgroundMgr:   BackgroundManager

    var onOpenSettings: () -> Void

    // MARK: - Local state

    @StateObject private var inferenceLoop: InferenceLoop = {
        // InferenceLoop constructed lazily with real services; see onAppear
        InferenceLoop(
            detector: SpeedCamDetector(),
            alertService: AlertService(),
            thermalManager: ThermalManager(),
            settings: AppSettings()
        )
    }()

    @State private var isScanning = false
    @State private var showPermissionAlert = false

    // MARK: - Body

    var body: some View {
        ZStack {
            // 1. Camera preview
            CameraPreviewRepresentable(session: cameraSession)
                .ignoresSafeArea()

            // 2. Overlay stack
            VStack(spacing: 0) {
                StatusOverlay(
                    scanState: inferenceLoop.scanState,
                    usingFallback: detector.usingFallback,
                    framesProcessed: inferenceLoop.framesProcessed
                )
                .padding(.top, 56)    // Below Dynamic Island on iPhone 15+

                Spacer()

                // Detection flash ring (visible for 1 second on detection)
                if inferenceLoop.scanState == .detected {
                    DetectionRing()
                        .transition(.opacity)
                }

                Spacer()

                // 3. Bottom control bar
                BottomBar(
                    isScanning: isScanning,
                    onToggle: { isScanning ? stopScanning() : startScanning() },
                    onSettings: onOpenSettings
                )
                .padding(.bottom, 32)
            }
        }
        .onAppear {
            cameraSession.requestPermissionAndStart()
            wireCameraToLoop()
            startScanning()
        }
        .onChange(of: cameraSession.permissionDenied) { denied in
            if denied { showPermissionAlert = true }
        }
        .onChange(of: inferenceLoop.scanState) { state in
            // Sync local scanning flag
            if state == .idle { isScanning = false }
        }
        .alert("Camera Access Required", isPresented: $showPermissionAlert) {
            Button("Open Settings") {
                if let url = URL(string: UIApplication.openSettingsURLString) {
                    UIApplication.shared.open(url)
                }
            }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("Spot-SpeedCam needs rear camera access to scan for mobile speed cameras.")
        }
    }

    // MARK: - Control

    private func startScanning() {
        inferenceLoop.start()
        cameraSession.resume()
        isScanning = true
    }

    private func stopScanning() {
        inferenceLoop.stop()
        isScanning = false
    }

    // MARK: - Wiring

    /// Connect CameraSession frame delivery → InferenceLoop.
    private func wireCameraToLoop() {
        cameraSession.onFrame = { [weak inferenceLoop] pixelBuffer in
            inferenceLoop?.processFrame(pixelBuffer)
        }

        backgroundMgr.setRestartHandler { [weak self] in
            guard let self else { return }
            Task { @MainActor in
                self.startScanning()
            }
        }
    }
}

// MARK: - CameraPreviewRepresentable

/// Bridges AVCaptureVideoPreviewLayer into SwiftUI.
private struct CameraPreviewRepresentable: UIViewRepresentable {
    let session: CameraSession

    func makeUIView(context: Context) -> PreviewUIView {
        let view = PreviewUIView()
        view.backgroundColor = .black
        return view
    }

    func updateUIView(_ uiView: PreviewUIView, context: Context) {
        if let layer = session.previewLayer {
            uiView.setPreviewLayer(layer)
        }
    }
}

private final class PreviewUIView: UIView {
    private var currentLayer: AVCaptureVideoPreviewLayer?

    func setPreviewLayer(_ layer: AVCaptureVideoPreviewLayer) {
        currentLayer?.removeFromSuperlayer()
        layer.frame = bounds
        layer.videoGravity = .resizeAspectFill
        self.layer.insertSublayer(layer, at: 0)
        currentLayer = layer
    }

    override func layoutSubviews() {
        super.layoutSubviews()
        currentLayer?.frame = bounds
    }
}

// MARK: - DetectionRing

/// Animated pulsing ring shown momentarily when a speed camera is detected.
private struct DetectionRing: View {
    @State private var scale: CGFloat = 0.8
    @State private var opacity: Double = 1.0

    var body: some View {
        Circle()
            .stroke(Color.red, lineWidth: 4)
            .frame(width: 200, height: 200)
            .scaleEffect(scale)
            .opacity(opacity)
            .onAppear {
                withAnimation(.easeOut(duration: 0.8).repeatCount(2, autoreverses: false)) {
                    scale = 1.4
                    opacity = 0
                }
            }
    }
}

// MARK: - BottomBar

private struct BottomBar: View {
    let isScanning: Bool
    let onToggle: () -> Void
    let onSettings: () -> Void

    var body: some View {
        HStack(spacing: 48) {
            Spacer()

            // Scan toggle button
            Button(action: onToggle) {
                ZStack {
                    Circle()
                        .fill(isScanning ? Color.red : Color.white)
                        .frame(width: 72, height: 72)
                        .shadow(radius: 8)
                    Image(systemName: isScanning ? "stop.fill" : "antenna.radiowaves.left.and.right")
                        .font(.system(size: 28, weight: .semibold))
                        .foregroundColor(isScanning ? .white : .black)
                }
            }
            .accessibilityLabel(isScanning ? "Stop scanning" : "Start scanning")

            // Settings
            Button(action: onSettings) {
                Image(systemName: "gearshape.fill")
                    .font(.system(size: 24, weight: .medium))
                    .foregroundColor(.white.opacity(0.85))
            }
            .accessibilityLabel("Settings")

            Spacer()
        }
    }
}
