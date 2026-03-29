import SwiftUI

// MARK: - StatusOverlay

/// Compact heads-up display pinned to the top of CameraView.
/// Shows: scan state badge | fallback warning | thermal warning | frame counter.
struct StatusOverlay: View {
    let scanState: ScanState
    let usingFallback: Bool
    let framesProcessed: Int

    @EnvironmentObject private var thermalManager: ThermalManager

    var body: some View {
        VStack(spacing: 8) {
            // App title pill
            HStack(spacing: 6) {
                Image(systemName: "camera.metering.spot")
                    .font(.system(size: 13, weight: .semibold))
                Text("Spot-SpeedCam")
                    .font(.system(size: 13, weight: .semibold))
            }
            .foregroundColor(.white)
            .padding(.horizontal, 14)
            .padding(.vertical, 6)
            .background(.ultraThinMaterial, in: Capsule())

            // Scan state badge
            ScanStateBadge(state: scanState)

            // Thermal warning
            if thermalManager.isSerious {
                ThermalBanner(label: thermalManager.statusLabel, isCritical: thermalManager.isCritical)
            }

            // CLIP fallback warning
            if usingFallback {
                FallbackBanner()
            }

            // Frame counter (debug — visible in debug builds only)
            #if DEBUG
            Text("Frames: \(framesProcessed)")
                .font(.system(size: 10, design: .monospaced))
                .foregroundColor(.white.opacity(0.5))
            #endif
        }
        .padding(.horizontal, 16)
    }
}

// MARK: - ScanStateBadge

private struct ScanStateBadge: View {
    let state: ScanState

    var body: some View {
        HStack(spacing: 6) {
            Circle()
                .fill(dotColor)
                .frame(width: 8, height: 8)
                .overlay(
                    Group {
                        if state == .scanning {
                            PulsingDot()
                        }
                    }
                )
            Text(label)
                .font(.system(size: 12, weight: .medium))
                .foregroundColor(.white)
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 5)
        .background(background, in: Capsule())
    }

    private var label: String {
        switch state {
        case .idle:            return "Stopped"
        case .scanning:        return "Scanning…"
        case .detected:        return "⚠ Speed Camera Detected"
        case .throttled:       return "Throttled"
        case .error(let msg):  return "Error: \(msg)"
        }
    }

    private var dotColor: Color {
        switch state {
        case .scanning:  return .green
        case .detected:  return .red
        case .throttled: return Color(hex: "FF6D00")
        case .error:     return .red
        case .idle:      return .gray
        }
    }

    private var background: some ShapeStyle {
        switch state {
        case .detected: return AnyShapeStyle(Color.red.opacity(0.85))
        default:        return AnyShapeStyle(.ultraThinMaterial)
        }
    }
}

// MARK: - PulsingDot

private struct PulsingDot: View {
    @State private var scale: CGFloat = 1.0

    var body: some View {
        Circle()
            .fill(Color.green.opacity(0.4))
            .frame(width: 16, height: 16)
            .scaleEffect(scale)
            .onAppear {
                withAnimation(.easeInOut(duration: 1.0).repeatForever(autoreverses: true)) {
                    scale = 1.5
                }
            }
    }
}

// MARK: - ThermalBanner

struct ThermalBanner: View {
    let label: String
    let isCritical: Bool

    var body: some View {
        HStack(spacing: 6) {
            Image(systemName: isCritical ? "thermometer.high" : "thermometer.medium")
            Text(label)
                .font(.system(size: 11, weight: .medium))
        }
        .foregroundColor(.white)
        .padding(.horizontal, 12)
        .padding(.vertical, 5)
        .background(isCritical ? Color.red.opacity(0.85) : Color(hex: "FF6D00").opacity(0.85), in: Capsule())
    }
}

// MARK: - FallbackBanner

private struct FallbackBanner: View {
    @EnvironmentObject private var settings: AppSettings

    var body: some View {
        if !settings.clipWarningDismissed {
            HStack(spacing: 6) {
                Image(systemName: "exclamationmark.triangle")
                    .font(.system(size: 11))
                Text("Using CLIP fallback — add CoreML model for best results")
                    .font(.system(size: 10))
                Spacer()
                Button {
                    settings.clipWarningDismissed = true
                } label: {
                    Image(systemName: "xmark")
                        .font(.system(size: 10, weight: .bold))
                }
            }
            .foregroundColor(.white)
            .padding(.horizontal, 12)
            .padding(.vertical, 6)
            .background(Color(hex: "E65100").opacity(0.9), in: RoundedRectangle(cornerRadius: 10))
            .padding(.horizontal, 8)
        }
    }
}

// MARK: - Color hex helper

extension Color {
    init(hex: String) {
        let hex = hex.trimmingCharacters(in: .alphanumerics.inverted)
        var int: UInt64 = 0
        Scanner(string: hex).scanHexInt64(&int)
        let r = Double((int >> 16) & 0xFF) / 255
        let g = Double((int >> 8)  & 0xFF) / 255
        let b = Double(int         & 0xFF) / 255
        self.init(red: r, green: g, blue: b)
    }
}
