import SwiftUI
import AVFoundation

struct CameraView: View {

    let query: SearchQuery?
    var onFrameCaptured: (UIImage) -> Void
    var onBack: () -> Void

    @StateObject private var cameraSession  = CameraSession()
    @StateObject private var thermalManager = ThermalStateManager()
    @State private var isCapturing = false

    var body: some View {
        ZStack {
            // Live camera feed
            if cameraSession.permissionGranted {
                CameraPreviewView(session: cameraSession.captureSession)
                    .ignoresSafeArea()
            } else {
                Color.black.ignoresSafeArea()
                permissionMessage
            }

            // HUD overlay
            VStack {
                topBar
                Spacer()
                if thermalManager.isCritical { WarmingUpIndicator() }
                ReticleView()
                Spacer()
                ScanButton(isCapturing: isCapturing, action: captureFrame)
                    .padding(.bottom, 50)
                    .disabled(thermalManager.isCritical || isCapturing)
            }
        }
        .task {
            await cameraSession.configure()
            cameraSession.start()
        }
        .onDisappear { cameraSession.stop() }
        .preferredColorScheme(.dark)
    }

    // MARK: - Sub-views

    private var topBar: some View {
        HStack {
            Button(action: onBack) {
                Image(systemName: "chevron.left")
                    .font(.title2)
                    .foregroundColor(.white)
                    .padding(12)
                    .background(Color.black.opacity(0.4))
                    .clipShape(Circle())
            }
            Spacer()
            if let query { QueryChip(text: query.text) }
        }
        .padding()
    }

    private var permissionMessage: some View {
        VStack(spacing: 12) {
            Image(systemName: "camera.fill")
                .font(.system(size: 48))
                .foregroundStyle(.secondary)
            Text("Camera access required")
                .foregroundColor(.white)
            Text("Enable camera permission in Settings.")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    // MARK: - Capture

    private func captureFrame() {
        guard !isCapturing else { return }
        isCapturing = true
        Task {
            do {
                let image = try await FrameGrabber.grab(from: cameraSession)
                await MainActor.run {
                    isCapturing = false
                    onFrameCaptured(image)
                }
            } catch {
                await MainActor.run { isCapturing = false }
            }
        }
    }
}

// MARK: - QueryChip

struct QueryChip: View {
    let text: String

    var body: some View {
        Text(text)
            .font(.caption.bold())
            .padding(.horizontal, 12)
            .padding(.vertical, 6)
            .background(Color.black.opacity(0.6))
            .foregroundColor(.white)
            .clipShape(Capsule())
            .overlay(Capsule().stroke(Color.white.opacity(0.3)))
    }
}

// MARK: - ReticleView

struct ReticleView: View {
    var body: some View {
        RoundedRectangle(cornerRadius: 8)
            .stroke(Color.white.opacity(0.5), lineWidth: 1.5)
            .frame(width: 240, height: 240)
    }
}

// MARK: - WarmingUpIndicator

struct WarmingUpIndicator: View {
    var body: some View {
        HStack(spacing: 8) {
            Image(systemName: "thermometer.high").foregroundColor(.red)
            Text("Device too warm — scanning paused")
                .font(.caption)
                .foregroundColor(.white)
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 8)
        .background(Color.red.opacity(0.2))
        .cornerRadius(8)
    }
}

// MARK: - ScanButton

struct ScanButton: View {
    let isCapturing: Bool
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            ZStack {
                Circle()
                    .stroke(Color.white, lineWidth: 3)
                    .frame(width: 72, height: 72)
                Circle()
                    .fill(isCapturing ? Color.gray : Color.white)
                    .frame(width: 60, height: 60)
            }
        }
    }
}
