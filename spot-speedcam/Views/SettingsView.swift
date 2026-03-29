import SwiftUI

// MARK: - SettingsView

/// Preference sheet. Slides up over CameraView — inference keeps running.
/// Much simpler than Sharp-I's settings: no iCloud, no mode toggles.
struct SettingsView: View {
    @EnvironmentObject private var settings: AppSettings
    @Environment(\.dismiss) private var dismiss

    @State private var hfKeyInput = ""
    @State private var showKeyField = false

    var body: some View {
        NavigationStack {
            Form {
                // MARK: Detection sensitivity
                Section("Detection") {
                    VStack(alignment: .leading, spacing: 4) {
                        HStack {
                            Text("Confidence threshold")
                            Spacer()
                            Text(String(format: "%.0f%%", settings.confidenceThreshold * 100))
                                .foregroundColor(.secondary)
                        }
                        Slider(
                            value: $settings.confidenceThreshold,
                            in: 0.45...0.90,
                            step: 0.05
                        )
                        Text("Higher = fewer false positives. Lower = catches more cameras.")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }

                    VStack(alignment: .leading, spacing: 4) {
                        HStack {
                            Text("Frame scan rate")
                            Spacer()
                            Text(frameSkipLabel)
                                .foregroundColor(.secondary)
                        }
                        Picker("Frame skip", selection: $settings.frameSkip) {
                            Text("High (every 3rd)").tag(3)
                            Text("Normal (every 5th)").tag(5)
                            Text("Low (every 10th)").tag(10)
                        }
                        .pickerStyle(.segmented)
                    }
                }

                // MARK: Alerts
                Section("Alert") {
                    Toggle("Audio alert", isOn: $settings.audioAlertEnabled)

                    VStack(alignment: .leading, spacing: 4) {
                        HStack {
                            Text("Re-alert cooldown")
                            Spacer()
                            Text(String(format: "%.0fs", settings.cooldownSeconds))
                                .foregroundColor(.secondary)
                        }
                        Slider(
                            value: $settings.cooldownSeconds,
                            in: 3.0...30.0,
                            step: 1.0
                        )
                        Text("Minimum seconds between repeated alerts for the same vehicle.")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                }

                // MARK: CLIP fallback (HuggingFace)
                Section {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Used while the CoreML model is being trained. Requires network. Adds ~600 ms latency.")
                            .font(.caption)
                            .foregroundColor(.secondary)

                        Button(showKeyField ? "Hide API key" : "Set HuggingFace API key") {
                            showKeyField.toggle()
                        }

                        if showKeyField {
                            SecureField("hf_…", text: $hfKeyInput)
                                .textContentType(.password)
                                .autocorrectionDisabled()

                            HStack {
                                Button("Save") {
                                    settings.saveHuggingFaceKey(hfKeyInput)
                                    hfKeyInput = ""
                                    showKeyField = false
                                }
                                .disabled(hfKeyInput.trimmingCharacters(in: .whitespaces).isEmpty)

                                Spacer()

                                Button("Delete", role: .destructive) {
                                    settings.deleteHuggingFaceKey()
                                    hfKeyInput = ""
                                    showKeyField = false
                                }
                            }
                        }
                    }
                } header: {
                    Text("CLIP Fallback (HuggingFace)")
                }

                // MARK: Model status
                Section("Model") {
                    ModelStatusRow()
                }

                // MARK: About
                Section("About") {
                    LabeledContent("Version", value: appVersion)
                    LabeledContent("Build", value: buildNumber)
                }
            }
            .navigationTitle("Settings")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }

    // MARK: - Helpers

    private var frameSkipLabel: String {
        switch settings.frameSkip {
        case 3:  return "High"
        case 5:  return "Normal"
        case 10: return "Low"
        default: return "\(settings.frameSkip)"
        }
    }

    private var appVersion: String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "—"
    }

    private var buildNumber: String {
        Bundle.main.infoDictionary?["CFBundleVersion"] as? String ?? "—"
    }
}

// MARK: - ModelStatusRow

private struct ModelStatusRow: View {
    private var modelPresent: Bool {
        Bundle.main.url(forResource: "SpeedCamDetector", withExtension: "mlpackage") != nil ||
        Bundle.main.url(forResource: "SpeedCamDetector", withExtension: "mlmodelc") != nil
    }

    var body: some View {
        HStack {
            Image(systemName: modelPresent ? "checkmark.seal.fill" : "exclamationmark.triangle.fill")
                .foregroundColor(modelPresent ? .green : .orange)
            VStack(alignment: .leading, spacing: 2) {
                Text(modelPresent ? "CoreML model loaded" : "CoreML model not found")
                    .font(.subheadline)
                Text(modelPresent
                     ? "On-device YOLOv8 — fast, private, works offline."
                     : "Using CLIP fallback. Add SpeedCamDetector.mlpackage to bundle.")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
        }
    }
}
